package de.hhu.bsinfo.dxram.logger.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class TcmdChangeLogLevel extends TerminalCommand{

	private static final Argument MS_ARG_LEVEL = new Argument("level", null, false, "available LogLevels: DISABLED ERROR WARN INFO DEBUG TRACE");
	
	@Override
	public String getName() {
		
		return "change-log-level";
	}

	@Override
	public String getDescription() {
		
		return "changes log level via terminal";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_LEVEL);	
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		String level =  p_arguments.getArgumentValue(MS_ARG_LEVEL, String.class);
		
		if(level == null)
			return false;
		
		LoggerService logService = getTerminalDelegate().getDXRAMService(LoggerService.class);
		logService.setLogLevel(level);
		
		return true;
	}

	
}
