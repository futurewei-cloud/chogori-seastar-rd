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
