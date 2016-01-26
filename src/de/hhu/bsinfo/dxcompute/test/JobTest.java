package de.hhu.bsinfo.dxcompute.test;

import de.hhu.bsinfo.dxcompute.ComputeJob;
import de.hhu.bsinfo.dxcompute.job.Job;
import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;

public class JobTest extends ComputeJob
{
	private int m_id;
	private long m_timeToWait;
	
	public JobTest(final int p_id, final long p_timeToWait)
	{
		m_id = p_id;
		m_timeToWait = p_timeToWait;
	}
	
	@Override
	public long getJobID() {
		return m_id;
	}
	
	@Override
	protected void execute() 
	{	
		try {
			log(LOG_LEVEL.LL_DEBUG, "Sleeping " + m_timeToWait);
			Thread.sleep(m_timeToWait);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}