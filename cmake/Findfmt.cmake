#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2020 Futurewei Technologies, Inc.
#

find_library (fmt_LIBRARY
  NAMES fmt)

find_path (fmt_INCLUDE_DIR
  NAMES fmt/format.h
  PATH_SUFFIXES fmt)

mark_as_advanced (
  fmt_LIBRARY
  fmt_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (fmt
  REQUIRED_VARS
    fmt_LIBRARY
    fmt_INCLUDE_DIR)

set (fmt_LIBRARIES ${fmt_LIBRARY})
set (fmt_INCLUDE_DIRS ${fmt_INCLUDE_DIR})

if (fmt_FOUND AND NOT (TARGET fmt::fmt))
  add_library (fmt::fmt UNKNOWN IMPORTED)

  set_target_properties (fmt::fmt
    PROPERTIES
      IMPORTED_LOCATION ${fmt_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${fmt_INCLUDE_DIRS}
      INTERFACE_LINK_LIBRARIES "${fmt_LIBRARIES}")
endif ()
