package de.mbe1224.utils.cli;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

public interface BaseCliCommand {

    static boolean handleParserExcepiton(String[] args, ArgumentParser parser, ArgumentParserException e) {
        if (args.length == 0) {
            parser.printHelp();
            return true;
        } else {
            parser.handleError(e);
            return false;
        }
    }
}
