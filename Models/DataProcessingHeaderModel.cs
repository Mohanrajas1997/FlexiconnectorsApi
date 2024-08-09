using System;

namespace MysqlEfCoreDemo.Models
{
	public class DataProcessingHeaderModel
	{
		public Int64 dataprocessingheader_gid { get; set; } = 0;
		public string dataprocessingheader_pipeline_code { get; set; }
        public Int64 dataprocessingheader_pplfieldmapping_gid { get; set; } = 0;
        public int dataprocessingheader_seqno { get; set; }
		public string dataprocessingheader_ppl_field_name { get; set; }
		public char delete_flag { get; set; } = 'N';

	}
}
