using System;

namespace MysqlEfCoreDemo.Models
{
    public class DataProcessingModel
    {
        public Int64 dataprocessing_gid { get; set; } = 0;
		public Int64 dataprocessing_header_gid { get; set; } = 0;
		public Int64 dataprocessing_child_master_gid { get; set; } = 0;
        public Int64 dataprocessing_parent_master_gid { get; set; } = 0;
        public Int64 dataprocessing_master_gid { get; set; } = 0;
        public int dataprocessing_pipeline_gid { get; set; } = 0;
        public int dataprocessing_pplfieldmapping_gid { get; set; } = 0;
        public int dataprocessing_orderby { get; set; } = 0;
        public string dataprocessing_param1 { get; set; } = null;
        public string dataprocessing_param2 { get; set; } = null;
		public string dataprocessing_param3 { get; set; } = null;
		public string dataprocessing_child_master_code { get; set; } = null;
        public string dataprocessing_parent_master_code { get; set; } = null;
        public string dataprocessing_master_code { get; set; } = null;
        public string dataprocessing_child_master_name { get; set; } = null;
        public string dataprocessing_parent_master_name { get; set; } = null;
        public string dataprocessing_master_name { get; set; } = null;
        public string dataprocessing_ppl_field_name { get; set; } = null;
        public char delete_flag { get; set; } = 'N';
    }
}
