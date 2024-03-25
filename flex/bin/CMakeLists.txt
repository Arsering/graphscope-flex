find_package(Hiactor)
set(WITH_TRICACHE ON)
if(Hiactor_FOUND)
        add_executable(rt_server rt_server.cc)
        target_link_libraries(rt_server flex_utils flex_rt_mutable_graph flex_graph_db flex_server)

       

        if(WITH_TRICACHE)
                target_link_libraries(rt_server pthread)
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")  # 设置 OpenMP 编译选项
                find_package(OpenMP REQUIRED)  # 查找 OpenMP
                target_link_libraries(rt_server Threads::Threads OpenMP::OpenMP_CXX)
                # if(NOT ${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
                #         message(FATAL_ERROR "TriCache requires Clang 13.0.1.")
                # endif()
                message(STATUS "Enabling TriCache.")
                set(CMAKE_AR llvm-ar-13)
                set(CMAKE_RANLIB llvm-ranlib-13)
                set(TRICACHE_DIR /data-1/yichengzhang/data/TriCache_space/TriCache/build CACHE STRING "build directory of TriCache")
                target_compile_options(rt_server PUBLIC -flto=thin -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheInstrument/TriCacheInstrument.so -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheAlloc/TriCacheAlloc.so)
                target_link_directories(rt_server PUBLIC ${TRICACHE_DIR})

                target_link_libraries(rt_server -flto=thin cache)

                #这里Tricache link了一遍了这边还要link一遍吗？
                find_package(PkgConfig)
                pkg_check_modules(SPDK REQUIRED spdk_nvme spdk_env_dpdk spdk_syslibs)
                link_directories(${SPDK_LIBRARY_DIRS})
                message("(${SPDK_INCLUDE_DIRS})")
                message("(${SPDK_LIBRARIES_DIRS})")
                message("(${SPDK_LIBRARIES})")
                target_link_directories(rt_server PUBLIC ${SPDK_LIBRARY_DIRS})
                target_link_libraries(rt_server -Wl,--whole-archive ${SPDK_LIBRARIES} -Wl,--no-whole-archive)
                target_link_libraries(rt_server boost_fiber boost_context boost_thread boost_filesystem boost_atomic numa dl)
        endif()

        install(TARGETS rt_server
                RUNTIME DESTINATION bin
                ARCHIVE DESTINATION lib
                LIBRARY DESTINATION lib)
endif()

add_executable(rt_admin rt_admin.cc)
target_link_libraries(rt_admin flex_utils flex_rt_mutable_graph flex_graph_db)

install(TARGETS rt_admin
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

add_executable(flex_analytical_engine flex_analytical_engine.cc)
target_link_libraries(flex_analytical_engine flex_immutable_graph flex_bsp ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})

install(TARGETS flex_analytical_engine
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

if(BUILD_HQPS)
        if(Hiactor_FOUND)
                add_executable(sync_server sync_server.cc)
                target_link_libraries(sync_server flex_utils flex_graph_db flex_server hqps_plan_proto ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})

                install(TARGETS sync_server
                        RUNTIME DESTINATION bin
                        ARCHIVE DESTINATION lib
                        LIBRARY DESTINATION lib)
        endif()
        # install the script
        install(PROGRAMS load_plan_and_gen.sh DESTINATION bin)
endif()

find_package(Boost COMPONENTS program_options REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader flex_rt_mutable_graph flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})

install(TARGETS bulk_loader
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)