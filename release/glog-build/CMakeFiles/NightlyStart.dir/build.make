# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /mnt/c/Users/Eden_CY/Desktop/NEW/minisql

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release

# Utility rule file for NightlyStart.

# Include the progress variables for this target.
include glog-build/CMakeFiles/NightlyStart.dir/progress.make

glog-build/CMakeFiles/NightlyStart:
	cd /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release/glog-build && /usr/bin/ctest -D NightlyStart

NightlyStart: glog-build/CMakeFiles/NightlyStart
NightlyStart: glog-build/CMakeFiles/NightlyStart.dir/build.make

.PHONY : NightlyStart

# Rule to build all files generated by this target.
glog-build/CMakeFiles/NightlyStart.dir/build: NightlyStart

.PHONY : glog-build/CMakeFiles/NightlyStart.dir/build

glog-build/CMakeFiles/NightlyStart.dir/clean:
	cd /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/NightlyStart.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/NightlyStart.dir/clean

glog-build/CMakeFiles/NightlyStart.dir/depend:
	cd /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/c/Users/Eden_CY/Desktop/NEW/minisql /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/thirdparty/glog /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release/glog-build /mnt/c/Users/Eden_CY/Desktop/NEW/minisql/release/glog-build/CMakeFiles/NightlyStart.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/NightlyStart.dir/depend

