/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/cacheline.hh>
#include <seastar/core/circular_buffer_fixed_capacity.hh>
#include <memory>
#include <type_traits>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unordered_map>
#include <netinet/ip.h>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <unistd.h>
#include <vector>
#include <queue>
#include <algorithm>
#include <thread>
#include <system_error>
#include <chrono>
#include <ratio>
#include <atomic>
#include <stack>
#include <seastar/util/std-compat.hh>
#include <boost/next_prior.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/container/static_vector.hpp>
#include <set>
#include <seastar/core/reactor_config.hh>
#include <seastar/core/linux-aio.hh>
#include <seastar/util/eclipse.hh>
#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/apply.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/enum.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <boost/range/irange.hpp>
#include <seastar/core/timer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/util/log.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/smp.hh>
#include "internal/pollable_fd.hh"
#include "internal/poll.hh"

#ifdef HAVE_OSV
#include <osv/sched.hh>
#include <osv/mutex.h>
#include <osv/condvar.h>
#include <osv/newpoll.hh>
#endif

struct _Unwind_Exception;
extern "C" int _Unwind_RaiseException(struct _Unwind_Exception *h);

namespace seastar {

using shard_id = unsigned;

namespace alien {
class message_queue;
}
class reactor;
inline
size_t iovec_len(const std::vector<iovec>& iov)
{
    size_t ret = 0;
    for (auto&& e : iov) {
        ret += e.iov_len;
    }
    return ret;
}

}

namespace std {

template <>
struct hash<::sockaddr_in> {
    size_t operator()(::sockaddr_in a) const {
        return a.sin_port ^ a.sin_addr.s_addr;
    }
};

}

bool operator==(const ::sockaddr_in a, const ::sockaddr_in b);

namespace seastar {

void register_network_stack(sstring name, boost::program_options::options_description opts,
    noncopyable_function<future<std::unique_ptr<network_stack>>(boost::program_options::variables_map opts)> create,
    bool make_default = false);

class thread_pool;
class smp;
namespace rdma {
class RDMAStack;
class RDMAConnection;
}

class reactor_backend_selector;

class reactor_backend;

namespace internal {

class reactor_stall_sampler;
class cpu_stall_detector;

}

class io_desc;
class io_queue;
class disk_config_params;

class reactor {
    using sched_clock = std::chrono::steady_clock;
private:
    struct task_queue;
    using task_queue_list = circular_buffer_fixed_capacity<task_queue*, max_scheduling_groups()>;
    using pollfn = seastar::pollfn;

    class io_pollfn;
    class signal_pollfn;
    class aio_batch_submit_pollfn;
    class batch_flush_pollfn;
    class smp_pollfn;
    class drain_cross_cpu_freelist_pollfn;
    class lowres_timer_pollfn;
    class manual_timer_pollfn;
    class epoll_pollfn;
    class syscall_pollfn;
    class execution_stage_pollfn;
    friend io_pollfn;
    friend signal_pollfn;
    friend aio_batch_submit_pollfn;
    friend batch_flush_pollfn;
    friend smp_pollfn;
    friend drain_cross_cpu_freelist_pollfn;
    friend lowres_timer_pollfn;
    friend class manual_clock;
    friend class epoll_pollfn;
    friend class syscall_pollfn;
    friend class execution_stage_pollfn;
    friend class file_data_source_impl; // for fstream statistics
    friend class internal::reactor_stall_sampler;
    friend class reactor_backend_epoll;
    friend class reactor_backend_aio;
    friend class rdma::RDMAConnection;
    friend class rdma::RDMAStack;
public:
    class poller {
        std::unique_ptr<pollfn> _pollfn;
        class registration_task;
        class deregistration_task;
        registration_task* _registration_task;
    public:
        template <typename Func> // signature: bool ()
        static poller simple(Func&& poll) {
            return poller(make_pollfn(std::forward<Func>(poll)));
        }
        poller(std::unique_ptr<pollfn> fn)
                : _pollfn(std::move(fn)) {
            do_register();
        }
        ~poller();
        poller(poller&& x);
        poller& operator=(poller&& x);
        void do_register();
        friend class reactor;
    };
    enum class idle_cpu_handler_result {
        no_more_work,
        interrupted_by_higher_priority_task
    };
    using work_waiting_on_reactor = const noncopyable_function<bool()>&;
    using idle_cpu_handler = noncopyable_function<idle_cpu_handler_result(work_waiting_on_reactor)>;

    struct io_stats {
        uint64_t aio_reads = 0;
        uint64_t aio_read_bytes = 0;
        uint64_t aio_writes = 0;
        uint64_t aio_write_bytes = 0;
        uint64_t aio_errors = 0;
        uint64_t fstream_reads = 0;
        uint64_t fstream_read_bytes = 0;
        uint64_t fstream_reads_blocked = 0;
        uint64_t fstream_read_bytes_blocked = 0;
        uint64_t fstream_read_aheads_discarded = 0;
        uint64_t fstream_read_ahead_discarded_bytes = 0;
    };

    std::unique_ptr<rdma::RDMAStack> _rdma_stack;
private:
    reactor_config _cfg;
    file_desc _notify_eventfd;
    file_desc _task_quota_timer;
#ifdef HAVE_OSV
    reactor_backend_osv _backend;
    sched::thread _timer_thread;
    sched::thread *_engine_thread;
    mutable mutex _timer_mutex;
    condvar _timer_cond;
    s64 _timer_due = 0;
#else
    std::unique_ptr<reactor_backend> _backend;
#endif
    sigset_t _active_sigmask; // holds sigmask while sleeping with sig disabled
    std::vector<pollfn*> _pollers;

    static constexpr unsigned max_aio_per_queue = 128;
    static constexpr unsigned max_queues = 8;
    static constexpr unsigned max_aio = max_aio_per_queue * max_queues;
    friend disk_config_params;
    // Not all reactors have IO queues. If the number of IO queues is less than the number of shards,
    // some reactors will talk to foreign io_queues. If this reactor holds a valid IO queue, it will
    // be stored here.
    std::vector<std::unique_ptr<io_queue>> my_io_queues;
    std::unordered_map<dev_t, io_queue*> _io_queues;
    friend io_queue;

    std::vector<noncopyable_function<future<> ()>> _exit_funcs;
    unsigned _id = 0;
    bool _stopping = false;
    bool _stopped = false;
    condition_variable _stop_requested;
    bool _handle_sigint = true;
    std::optional<future<std::unique_ptr<network_stack>>> _network_stack_ready;
    int _return = 0;
    promise<> _start_promise;
    semaphore _cpu_started;
    internal::preemption_monitor _preemption_monitor{};
    uint64_t _global_tasks_processed = 0;
    uint64_t _polls = 0;
    std::unique_ptr<internal::cpu_stall_detector> _cpu_stall_detector;

    unsigned _max_task_backlog = 1000;
    timer_set<timer<>, &timer<>::_link> _timers;
    timer_set<timer<>, &timer<>::_link>::timer_list_t _expired_timers;
    timer_set<timer<lowres_clock>, &timer<lowres_clock>::_link> _lowres_timers;
    timer_set<timer<lowres_clock>, &timer<lowres_clock>::_link>::timer_list_t _expired_lowres_timers;
    timer_set<timer<manual_clock>, &timer<manual_clock>::_link> _manual_timers;
    timer_set<timer<manual_clock>, &timer<manual_clock>::_link>::timer_list_t _expired_manual_timers;
    internal::linux_abi::aio_context_t _io_context;
    alignas(cache_line_size) std::array<internal::linux_abi::iocb, max_aio> _iocb_pool;
    std::stack<internal::linux_abi::iocb*, boost::container::static_vector<internal::linux_abi::iocb*, max_aio>> _free_iocbs;
    boost::container::static_vector<internal::linux_abi::iocb*, max_aio> _pending_aio;
    boost::container::static_vector<internal::linux_abi::iocb*, max_aio> _pending_aio_retry;
    io_stats _io_stats;
    uint64_t _fsyncs = 0;
    uint64_t _cxx_exceptions = 0;
    struct task_queue {
        explicit task_queue(unsigned id, sstring name, float shares);
        int64_t _vruntime = 0;
        float _shares;
        int64_t _reciprocal_shares_times_2_power_32;
        bool _current = false;
        bool _active = false;
        uint8_t _id;
        sched_clock::duration _runtime = {};
        uint64_t _tasks_processed = 0;
        circular_buffer<std::unique_ptr<task>> _q;
        sstring _name;
        /**
         * This array holds pointers to the scheduling group specific
         * data. The pointer is not use as is but is cast to a reference
         * to the appropriate type that is actually pointed to.
         */
        std::vector<void*> _scheduling_group_specific_vals;
        int64_t to_vruntime(sched_clock::duration runtime) const;
        void set_shares(float shares);
        struct indirect_compare;
        sched_clock::duration _time_spent_on_task_quota_violations = {};
        seastar::metrics::metric_groups _metrics;
        void rename(sstring new_name);
    private:
        void register_stats();
    };
    boost::container::static_vector<std::unique_ptr<task_queue>, max_scheduling_groups()> _task_queues;
    std::vector<scheduling_group_key_config> _scheduling_group_key_configs;
    int64_t _last_vruntime = 0;
    task_queue_list _active_task_queues;
    task_queue_list _activating_task_queues;
    task_queue* _at_destroy_tasks;
    sched_clock::duration _task_quota;
    /// Handler that will be called when there is no task to execute on cpu.
    /// It represents a low priority work.
    ///
    /// Handler's return value determines whether handler did any actual work. If no work was done then reactor will go
    /// into sleep.
    ///
    /// Handler's argument is a function that returns true if a task which should be executed on cpu appears or false
    /// otherwise. This function should be used by a handler to return early if a task appears.
    idle_cpu_handler _idle_cpu_handler{ [] (work_waiting_on_reactor) {return idle_cpu_handler_result::no_more_work;} };
    std::unique_ptr<network_stack> _network_stack;
    // _lowres_clock_impl will only be created on cpu 0
    std::unique_ptr<lowres_clock_impl> _lowres_clock_impl;
    lowres_clock::time_point _lowres_next_timeout;
    compat::optional<poller> _epoll_poller;
    compat::optional<pollable_fd> _aio_eventfd;
    const bool _reuseport;
    circular_buffer<double> _loads;
    double _load = 0;
    sched_clock::duration _total_idle;
    sched_clock::duration _total_sleep;
    sched_clock::time_point _start_time = sched_clock::now();
    std::chrono::nanoseconds _max_poll_time = calculate_poll_time();
    circular_buffer<output_stream<char>* > _flush_batching;
    std::atomic<bool> _sleeping alignas(seastar::cache_line_size);
    pthread_t _thread_id alignas(seastar::cache_line_size) = pthread_self();
    bool _strict_o_direct = true;
    bool _force_io_getevents_syscall = false;
    bool _bypass_fsync = false;
    bool _have_aio_fsync = false;
    std::atomic<bool> _dying{false};
private:
    static std::chrono::nanoseconds calculate_poll_time();
    static void block_notifier(int);
    void wakeup();
    size_t handle_aio_error(internal::linux_abi::iocb* iocb, int ec);
    bool flush_pending_aio();
    bool flush_tcp_batches();
    bool do_expire_lowres_timers();
    bool do_check_lowres_timers() const;
    void expire_manual_timers();
    void abort_on_error(int ret);
    void start_aio_eventfd_loop();
    void stop_aio_eventfd_loop();
    template <typename T, typename E, typename EnableFunc>
    void complete_timers(T&, E&, EnableFunc&& enable_fn);

    /**
     * Returns TRUE if all pollers allow blocking.
     *
     * @return FALSE if at least one of the blockers requires a non-blocking
     *         execution.
     */
    bool poll_once();
    bool pure_poll_once();
    template <typename Func> // signature: bool ()
    static std::unique_ptr<pollfn> make_pollfn(Func&& func);

public:
    /// Register a user-defined signal handler
    void handle_signal(int signo, noncopyable_function<void ()>&& handler);

private:
    class signals {
    public:
        signals();
        ~signals();

        bool poll_signal();
        bool pure_poll_signal() const;
        void handle_signal(int signo, noncopyable_function<void ()>&& handler);
        void handle_signal_once(int signo, noncopyable_function<void ()>&& handler);
        static void action(int signo, siginfo_t* siginfo, void* ignore);
        static void failed_to_handle(int signo);
    private:
        struct signal_handler {
            signal_handler(int signo, noncopyable_function<void ()>&& handler);
            noncopyable_function<void ()> _handler;
        };
        std::atomic<uint64_t> _pending_signals;
        std::unordered_map<int, signal_handler> _signal_handlers;

        friend void reactor::handle_signal(int, noncopyable_function<void ()>&&);
    };

    signals _signals;
    std::unique_ptr<thread_pool> _thread_pool;
    friend class thread_pool;
    friend class internal::cpu_stall_detector;

    uint64_t pending_task_count() const;
    void run_tasks(task_queue& tq);
    bool have_more_tasks() const;
    bool posix_reuseport_detect();
    void task_quota_timer_thread_fn();
    void run_some_tasks();
    void activate(task_queue& tq);
    void insert_active_task_queue(task_queue* tq);
    void insert_activating_task_queues();
    void account_runtime(task_queue& tq, sched_clock::duration runtime);
    void account_idle(sched_clock::duration idletime);
    void allocate_scheduling_group_specific_data(scheduling_group sg, scheduling_group_key key);
    future<> init_scheduling_group(scheduling_group sg, sstring name, float shares);
    future<> init_new_scheduling_group_key(scheduling_group_key key, scheduling_group_key_config cfg);
    future<> destroy_scheduling_group(scheduling_group sg);
    [[noreturn]] void no_such_scheduling_group(scheduling_group sg);
    void* get_scheduling_group_specific_value(scheduling_group sg, scheduling_group_key key) {
        if (!_task_queues[sg._id]) {
            no_such_scheduling_group(sg);
        }
        return _task_queues[sg._id]->_scheduling_group_specific_vals[key.id()];
    }
    void* get_scheduling_group_specific_value(scheduling_group_key key) {
        return get_scheduling_group_specific_value(*internal::current_scheduling_group_ptr(), key);
    }
    uint64_t tasks_processed() const;
    uint64_t min_vruntime() const;
    void request_preemption();
    void start_handling_signal();
    void reset_preemption_monitor();
    void service_highres_timer();
public:
    static boost::program_options::options_description get_options_description(reactor_config cfg);
    explicit reactor(unsigned id, reactor_backend_selector rbs, reactor_config cfg);
    reactor(const reactor&) = delete;
    ~reactor();
    void operator=(const reactor&) = delete;

    sched_clock::duration uptime() {
        return sched_clock::now() - _start_time;
    }

    io_queue& get_io_queue(dev_t devid = 0) {
        auto queue = _io_queues.find(devid);
        if (queue == _io_queues.end()) {
            return *_io_queues[0];
        } else {
            return *(queue->second);
        }
    }

    io_priority_class register_one_priority_class(sstring name, uint32_t shares);

    /// \brief Updates the current amount of shares for a given priority class
    ///
    /// This can involve a cross-shard call if the I/O Queue that is responsible for
    /// this class lives in a foreign shard.
    ///
    /// \param pc the priority class handle
    /// \param shares the new shares value
    /// \return a future that is ready when the share update is applied
    future<> update_shares_for_class(io_priority_class pc, uint32_t shares);
    static future<> rename_priority_class(io_priority_class pc, sstring new_name);

    void configure(boost::program_options::variables_map config);

    server_socket listen(socket_address sa, listen_options opts = {});

    future<connected_socket> connect(socket_address sa);
    future<connected_socket> connect(socket_address, socket_address, transport proto = transport::TCP);

    pollable_fd posix_listen(socket_address sa, listen_options opts = {});

    bool posix_reuseport_available() const { return _reuseport; }

    lw_shared_ptr<pollable_fd> make_pollable_fd(socket_address sa, int proto);

    future<> posix_connect(lw_shared_ptr<pollable_fd> pfd, socket_address sa, socket_address local);

    future<std::tuple<pollable_fd, socket_address>> accept(pollable_fd_state& listen_fd);

    future<size_t> read_some(pollable_fd_state& fd, void* buffer, size_t size);
    future<size_t> read_some(pollable_fd_state& fd, const std::vector<iovec>& iov);

    future<size_t> write_some(pollable_fd_state& fd, const void* buffer, size_t size);

    future<> write_all(pollable_fd_state& fd, const void* buffer, size_t size);

    future<file> open_file_dma(sstring name, open_flags flags, file_open_options options = {});
    future<file> open_directory(sstring name);
    future<> make_directory(sstring name, file_permissions permissions = file_permissions::default_dir_permissions);
    future<> touch_directory(sstring name, file_permissions permissions = file_permissions::default_dir_permissions);
    future<compat::optional<directory_entry_type>>  file_type(sstring name, follow_symlink = follow_symlink::yes);
    future<stat_data> file_stat(sstring pathname, follow_symlink);
    future<uint64_t> file_size(sstring pathname);
    future<bool> file_accessible(sstring pathname, access_flags flags);
    future<bool> file_exists(sstring pathname) {
        return file_accessible(pathname, access_flags::exists);
    }
    future<fs_type> file_system_at(sstring pathname);
    future<struct statvfs> statvfs(sstring pathname);
    future<> remove_file(sstring pathname);
    future<> rename_file(sstring old_pathname, sstring new_pathname);
    future<> link_file(sstring oldpath, sstring newpath);
    future<> chmod(sstring name, file_permissions permissions);

    // In the following three methods, prepare_io is not guaranteed to execute in the same processor
    // in which it was generated. Therefore, care must be taken to avoid the use of objects that could
    // be destroyed within or at exit of prepare_io.
    void submit_io(io_desc* desc,
            noncopyable_function<void (internal::linux_abi::iocb&)> prepare_io);
    future<internal::linux_abi::io_event> submit_io_read(io_queue* ioq,
            const io_priority_class& priority_class,
            size_t len,
            noncopyable_function<void (internal::linux_abi::iocb&)> prepare_io);
    future<internal::linux_abi::io_event> submit_io_write(io_queue* ioq,
            const io_priority_class& priority_class,
            size_t len,
            noncopyable_function<void (internal::linux_abi::iocb&)> prepare_io);

    inline void handle_io_result(const internal::linux_abi::io_event& ev) {
        auto res = long(ev.res);
        if (res < 0) {
            ++_io_stats.aio_errors;
            throw_kernel_error(res);
        }
    }

    int run();
    void exit(int ret);
    future<> when_started() { return _start_promise.get_future(); }
    // The function waits for timeout period for reactor stop notification
    // which happens on termination signals or call for exit().
    template <typename Rep, typename Period>
    future<> wait_for_stop(std::chrono::duration<Rep, Period> timeout) {
        return _stop_requested.wait(timeout, [this] { return _stopping; });
    }

    void at_exit(noncopyable_function<future<> ()> func);

    template <typename Func>
    void at_destroy(Func&& func) {
        _at_destroy_tasks->_q.push_back(make_task(default_scheduling_group(), std::forward<Func>(func)));
    }

#ifdef SEASTAR_SHUFFLE_TASK_QUEUE
    void shuffle(std::unique_ptr<task>&, task_queue&);
#endif

    void add_task(std::unique_ptr<task>&& t) {
        auto sg = t->group();
        auto* q = _task_queues[sg._id].get();
        bool was_empty = q->_q.empty();
        q->_q.push_back(std::move(t));
#ifdef SEASTAR_SHUFFLE_TASK_QUEUE
        shuffle(q->_q.back(), *q);
#endif
        if (was_empty) {
            activate(*q);
        }
    }
    void add_urgent_task(std::unique_ptr<task>&& t) {
        auto sg = t->group();
        auto* q = _task_queues[sg._id].get();
        bool was_empty = q->_q.empty();
        q->_q.push_front(std::move(t));
#ifdef SEASTAR_SHUFFLE_TASK_QUEUE
        shuffle(q->_q.front(), *q);
#endif
        if (was_empty) {
            activate(*q);
        }
    }

    /// Set a handler that will be called when there is no task to execute on cpu.
    /// Handler should do a low priority work.
    ///
    /// Handler's return value determines whether handler did any actual work. If no work was done then reactor will go
    /// into sleep.
    ///
    /// Handler's argument is a function that returns true if a task which should be executed on cpu appears or false
    /// otherwise. This function should be used by a handler to return early if a task appears.
    void set_idle_cpu_handler(idle_cpu_handler&& handler) {
        _idle_cpu_handler = std::move(handler);
    }
    void force_poll();

    void add_high_priority_task(std::unique_ptr<task>&&);

    network_stack& net() { return *_network_stack; }
    shard_id cpu_id() const { return _id; }

    void start_epoll();
    void sleep();

    steady_clock_type::duration total_idle_time();
    steady_clock_type::duration total_busy_time();
    std::chrono::nanoseconds total_steal_time();

    const io_stats& get_io_stats() const { return _io_stats; }
#ifdef HAVE_OSV
    void timer_thread_func();
    void set_timer(sched::timer &tmr, s64 t);
#endif
private:
    /**
     * Add a new "poller" - a non-blocking function returning a boolean, that
     * will be called every iteration of a main loop.
     * If it returns FALSE then reactor's main loop is forbidden to block in the
     * current iteration.
     *
     * @param fn a new "poller" function to register
     */
    void register_poller(pollfn* p);
    void unregister_poller(pollfn* p);
    void replace_poller(pollfn* old, pollfn* neww);
    void register_metrics();
    future<> write_all_part(pollable_fd_state& fd, const void* buffer, size_t size, size_t completed);

    bool process_io();

    future<> fdatasync(int fd);

    void add_timer(timer<steady_clock_type>*);
    bool queue_timer(timer<steady_clock_type>*);
    void del_timer(timer<steady_clock_type>*);
    void add_timer(timer<lowres_clock>*);
    bool queue_timer(timer<lowres_clock>*);
    void del_timer(timer<lowres_clock>*);
    void add_timer(timer<manual_clock>*);
    bool queue_timer(timer<manual_clock>*);
    void del_timer(timer<manual_clock>*);

    future<> run_exit_tasks();
    void stop();
    friend class alien::message_queue;
    friend class pollable_fd;
    friend class pollable_fd_state;
    friend class posix_file_impl;
    friend class blockdev_file_impl;
    friend class readable_eventfd;
    friend class timer<>;
    friend class timer<lowres_clock>;
    friend class timer<manual_clock>;
    friend class smp;
    friend class smp_message_queue;
    friend class poller;
    friend class scheduling_group;
    friend void add_to_flush_poller(output_stream<char>* os);
    friend int ::_Unwind_RaiseException(struct _Unwind_Exception *h);
    metrics::metric_groups _metric_groups;
    friend future<scheduling_group> create_scheduling_group(sstring name, float shares);
    friend future<> seastar::destroy_scheduling_group(scheduling_group);
    friend future<> seastar::rename_scheduling_group(scheduling_group sg, sstring new_name);
    friend future<scheduling_group_key> scheduling_group_key_create(scheduling_group_key_config cfg);

    template<typename T>
    friend T& scheduling_group_get_specific(scheduling_group sg, scheduling_group_key key);
    template<typename T>
    friend T& scheduling_group_get_specific(scheduling_group_key key);
    template<typename SpecificValType, typename Mapper, typename Reducer, typename Initial>
        GCC6_CONCEPT( requires requires(SpecificValType specific_val, Mapper mapper, Reducer reducer, Initial initial) {
            {reducer(initial, mapper(specific_val))} -> Initial;
        })
    friend future<typename function_traits<Reducer>::return_type>
    map_reduce_scheduling_group_specific(Mapper mapper, Reducer reducer, Initial initial_val, scheduling_group_key key);
    template<typename SpecificValType, typename Reducer, typename Initial>
    GCC6_CONCEPT( requires requires(SpecificValType specific_val, Reducer reducer, Initial initial) {
        {reducer(initial, specific_val)} -> Initial;
    })
    friend future<typename function_traits<Reducer>::return_type>
        reduce_scheduling_group_specific(Reducer reducer, Initial initial_val, scheduling_group_key key);
public:
    bool wait_and_process(int timeout = 0, const sigset_t* active_sigmask = nullptr);
    future<> readable(pollable_fd_state& fd);
    future<> writeable(pollable_fd_state& fd);
    future<> readable_or_writeable(pollable_fd_state& fd);
    void forget(pollable_fd_state& fd);
    void abort_reader(pollable_fd_state& fd);
    void abort_writer(pollable_fd_state& fd);
    void enable_timer(steady_clock_type::time_point when);
    /// Sets the "Strict DMA" flag.
    ///
    /// When true (default), file I/O operations must use DMA.  This is
    /// the most performant option, but does not work on some file systems
    /// such as tmpfs or aufs (used in some Docker setups).
    ///
    /// When false, file I/O operations can fall back to buffered I/O if
    /// DMA is not available.  This can result in dramatic reducation in
    /// performance and an increase in memory consumption.
    void set_strict_dma(bool value);
    void set_bypass_fsync(bool value);
    void update_blocked_reactor_notify_ms(std::chrono::milliseconds ms);
    std::chrono::milliseconds get_blocked_reactor_notify_ms() const;
    // For testing:
    void set_stall_detector_report_function(std::function<void ()> report);
    std::function<void ()> get_stall_detector_report_function() const;
};

template <typename Func> // signature: bool ()
inline
std::unique_ptr<reactor::pollfn>
reactor::make_pollfn(Func&& func) {
    struct the_pollfn : pollfn {
        the_pollfn(Func&& func) : func(std::forward<Func>(func)) {}
        Func func;
        virtual bool poll() override final {
            return func();
        }
        virtual bool pure_poll() override final {
            return poll(); // dubious, but compatible
        }
    };
    return std::make_unique<the_pollfn>(std::forward<Func>(func));
}

extern __thread reactor* local_engine;
extern __thread size_t task_quota;

inline reactor& engine() {
    return *local_engine;
}

inline bool engine_is_ready() {
    return local_engine != nullptr;
}

inline
pollable_fd_state::~pollable_fd_state() {
    engine().forget(*this);
}

inline
size_t iovec_len(const iovec* begin, size_t len)
{
    size_t ret = 0;
    auto end = begin + len;
    while (begin != end) {
        ret += begin++->iov_len;
    }
    return ret;
}

inline int alarm_signal() {
    // We don't want to use SIGALRM, because the boost unit test library
    // also plays with it.
    return SIGRTMIN;
}


extern logger seastar_logger;

}
