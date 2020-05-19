/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
//! \brief
//! Controller to start other Ml processes.
//!
//! DESCRIPTION:\n
//! Starts other Ml processes based on commands sent to it
//! through a named pipe.
//!
//! Each command has the following format:
//! verb arguments...
//!
//! These components must be separated using tabs, and the overall
//! command must be terminated with a newline.  (This implies that
//! keys and arguments cannot contain tabs or newlines.)
//!
//! IMPLEMENTATION DECISIONS:\n
//! Standalone program.
//!
//! Only accepts requests to start the following processes:
//! 1) ./autoconfig
//! 2) ./autodetect
//! 3) ./categorize
//! 4) ./data_frame_analyzer
//! 5) ./normalize
//!
//! The assumption here is that the working directory of this
//! process will be the directory containing these other
//! processes.
//!
//! Always logs to a named pipe and accepts commands from
//! a named pipe.
//!
//! Additionally, reads from STDIN and will exit when it detects
//! EOF on STDIN.  This is so that it can exit if the JVM that
//! started it dies before the command named pipe is set up.
//!
#include <core/CLogger.h>
#include <core/CNamedPipeFactory.h>
#include <core/COsFileFuncs.h>
#include <core/CProcess.h>
#include <core/CProgName.h>
#include <core/CStringUtils.h>
#include <core/CThread.h>

#include <ver/CBuildInfo.h>

#include "CBlockingCallCancellerThread.h"
#include "CCmdLineParser.h"
#include "CCommandProcessor.h"

#include <fstream>
#include <iostream>
#include <string>

#include <errno.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
    g_InitLog = new std::ofstream("/tmp/controller_init_log"); // g_InitLog in CLogger.h
    const std::string& defaultNamedPipePath = ml::core::CNamedPipeFactory::defaultPath();
    *g_InitLog << timeNowMs()
               << " defaultNamedPipePath = " << defaultNamedPipePath << std::endl;
    const std::string& progName = ml::core::CProgName::progName();

    // Read command line options
    std::string jvmPidStr = ml::core::CStringUtils::typeToString(
        ml::core::CProcess::instance().parentId());
    *g_InitLog << timeNowMs() << " jvmPidStr = " << jvmPidStr << std::endl;
    std::string logPipe;
    std::string commandPipe;
    if (ml::controller::CCmdLineParser::parse(argc, argv, jvmPidStr, logPipe,
                                              commandPipe) == false) {
        *g_InitLog << timeNowMs() << " exiting after parsing " << (argc - 1)
                   << " command line args" << std::endl;
        for (int i = 1; i < argc; ++i) {
            *g_InitLog << timeNowMs() << " $" << i << " = " << argv[i] << std::endl;
        }
        return EXIT_FAILURE;
    }

    if (logPipe.empty()) {
        logPipe = defaultNamedPipePath + progName + "_log_" + jvmPidStr;
    }
    if (commandPipe.empty()) {
        commandPipe = defaultNamedPipePath + progName + "_command_" + jvmPidStr;
    }
    *g_InitLog << timeNowMs() << " logPipe = " << logPipe << std::endl;
    *g_InitLog << timeNowMs() << " commandPipe = " << commandPipe << std::endl;

    // This needs to be started before reconfiguring logging just in case
    // nothing connects to the other end of the logging pipe.  This could
    // happen if say:
    // 1) The pre-seccomp code in the Java process starts this process
    // 2) A bootstrap check, e.g. jar hell, fails
    // 3) The Java process exits with an error status
    // 4) No plugin code ever runs
    // This thread will detect the death of the parent process because this
    // process's STDIN will be closed.
    ml::controller::CBlockingCallCancellerThread cancellerThread(
        ml::core::CThread::currentThreadId(), std::cin);
    *g_InitLog << timeNowMs() << " Constructed blocking call canceller thread" << std::endl;
    if (cancellerThread.start() == false) {
        *g_InitLog << timeNowMs() << " Could not start blocking call canceller thread"
                   << std::endl;
        // This log message will probably never been seen as it will go to the
        // real stderr of this process rather than the log pipe...
        LOG_FATAL(<< "Could not start blocking call canceller thread");
        return EXIT_FAILURE;
    }
    *g_InitLog << timeNowMs() << " Started blocking call canceller thread" << std::endl;

    if (ml::core::CLogger::instance().reconfigureLogToNamedPipe(logPipe) == false) {
        *g_InitLog << timeNowMs()
                   << " Could not reconfigure logging: " << ::strerror(errno)
                   << std::endl;
        LOG_FATAL(<< "Could not reconfigure logging");
        cancellerThread.stop();
        return EXIT_FAILURE;
    }
    *g_InitLog << timeNowMs() << " Reconfigured logging" << std::endl;

    // Log the program version immediately after reconfiguring the logger.  This
    // must be done from the program, and NOT a shared library, as each program
    // statically links its own version library.
    LOG_INFO(<< ml::ver::CBuildInfo::fullInfo());
    *g_InitLog << timeNowMs()
               << " Version info: " << ml::ver::CBuildInfo::fullInfo() << std::endl;

    // Unlike other programs we DON'T reduce the process priority here, because
    // the controller is critical to the overall system.  Also its resource
    // requirements should always be very low.

    ml::core::CNamedPipeFactory::TIStreamP commandStream =
        ml::core::CNamedPipeFactory::openPipeStreamRead(commandPipe);
    if (commandStream == nullptr) {
        *g_InitLog << timeNowMs()
                   << " Could not open command pipe: " << ::strerror(errno)
                   << std::endl;
        LOG_FATAL(<< "Could not open command pipe");
        cancellerThread.stop();
        return EXIT_FAILURE;
    }
    *g_InitLog << timeNowMs() << " Opened command pipe" << std::endl;

    // Change directory to the directory containing this program, because the
    // permitted paths all assume the current working directory contains the
    // permitted programs
    const std::string& progDir = ml::core::CProgName::progDir();
    *g_InitLog << timeNowMs() << " Will change directory to " << progDir << std::endl;
    if (ml::core::COsFileFuncs::chdir(progDir.c_str()) == -1) {
        *g_InitLog << timeNowMs() << " Could not change directory to '"
                   << progDir << "': " << ::strerror(errno) << std::endl;
        LOG_FATAL(<< "Could not change directory to '" << progDir
                  << "': " << ::strerror(errno));
        cancellerThread.stop();
        return EXIT_FAILURE;
    }
    *g_InitLog << timeNowMs() << " Changed directory to " << progDir << std::endl;

    ml::controller::CCommandProcessor::TStrVec permittedProcessPaths;
    permittedProcessPaths.push_back("./autoconfig");
    permittedProcessPaths.push_back("./autodetect");
    permittedProcessPaths.push_back("./categorize");
    permittedProcessPaths.push_back("./data_frame_analyzer");
    permittedProcessPaths.push_back("./normalize");

    *g_InitLog << timeNowMs() << " Constructing command processor" << std::endl;
    ml::controller::CCommandProcessor processor(permittedProcessPaths);
    *g_InitLog << timeNowMs() << " About to start command processor" << std::endl;
    processor.processCommands(*commandStream);
    *g_InitLog << timeNowMs() << " Started command processor" << std::endl;

    cancellerThread.stop();

    // This message makes it easier to spot process crashes in a log file - if
    // this isn't present in the log for a given PID and there's no other log
    // message indicating early exit then the process has probably core dumped
    LOG_INFO(<< "Ml controller exiting");
    *g_InitLog << timeNowMs() << " Ml controller exiting" << std::endl;

    return EXIT_SUCCESS;
}
