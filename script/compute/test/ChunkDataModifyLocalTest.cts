{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "ChunkDataModifyLocalTest",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		}
	]
}
