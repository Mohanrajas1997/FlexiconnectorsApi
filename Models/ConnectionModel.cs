using CsvHelper.Configuration.Attributes;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Graph.Models;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection.Metadata;

namespace MysqlEfCoreDemo.Models
{
    #region Table Structure
    public class ConnectionModel
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int connection_gid { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string connection_code { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string connection_name { get; set; }
        [Column(TypeName = "text")]
        public string connection_desc { get; set; }
        [Column(TypeName = "varchar(16)")]
        public string source_db_type { get; set; }
        [Column(TypeName = "char(1)")]
        public string protection_type { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string file_password { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string source_host_name { get; set; }
        [Column(TypeName = "varchar(8)")]
        public string source_port { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string source_auth_mode { get; set; }

        [Column(TypeName = "varchar(128)")]
        public string source_db_user { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string source_db_pwd { get; set; }
        [Column(TypeName = "text")]
        public string source_auth_file_name { get; set; }
        [Column(TypeName = "blob")]
        public byte[] source_auth_file_blob { get; set; }

        [Column(TypeName = "text")]
        public string source_file { get; set; }
        [Column(TypeName = "char(1)")]
        public string ssh_tunneling { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string ssh_host_name { get; set; }
        [Column(TypeName = "varchar(8)")]
        public string ssh_port { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string ssh_user { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string ssh_pwd { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string ssh_auth_mode { get; set; }
        [Column(TypeName = "text")]
        public string ssh_file_name { get; set; }
        [Column(TypeName = "blob")]
        public byte[] ssh_file_blob { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string connection_status { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class Pipeline
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int pipeline_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string pipeline_name { get; set; }
        [Column(TypeName = "text")]
        public string pipeline_desc { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string connection_code { get; set; }
        [Column(TypeName = "varchar(256)")]
        public string db_name { get; set; }
        [Column(TypeName = "varchar(256)")]
        public string source_file_name { get; set; }
        [Column(TypeName = "varchar(256)")]
        public string sheet_name { get; set; }
        [Column(TypeName = "char(1)")]
        public string table_view_query_type { get; set; }
        [Column(TypeName = "text")]
        public string table_view_query_desc { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string target_dataset_code { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string pipeline_status { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class PipelineSourcefield
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int pplsourcefield_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string sourcefield_name { get; set; }
        [Column(TypeName = "int(11)")]
        public int sourcefield_sno { get; set; } = 0;
        [Column(TypeName = "varchar(128)")]
        public string sourcefield_datatype { get; set; }
        [Column(TypeName = "text")]
        public string sourcefield_expression { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string source_type { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string dataset_table_field { get; set; }
        [Column(TypeName = "int(11)")]
        public int dataset_table_field_sno { get; set; }
        [Column(TypeName = "text")]
        public string cast_dataset_table_field { get; set; }
        [Column(TypeName = "json")]
        public string expressionfield_json { get; set; }
        [Column(TypeName = "char(1)")]
        public string sourcefieldmapping_flag { get; set; } = "N";
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }


    public class PipelineMapping
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int pplfieldmapping_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string dataset_code { get; set; }
        [Column(TypeName = "boolean")]
        public int pplfieldmapping_flag { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string ppl_field_name { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string dataset_field_name { get; set; }

        [Column(TypeName = "varchar(128)")]
        public string default_value { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class PipelineCondition
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int pplcondition_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string condition_type { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string condition_name { get; set; }
        [Column(TypeName = "text")]
        public string condition_text { get; set; }
        [Column(TypeName = "text")]
        public string condition_msg { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class IncrementalRecord
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int incremental_gid { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(255)")]
        public string incremental_field { get; set; }
        [Column(TypeName = "varchar(255)")]
        public string incremental_value { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class PipelineFinalization
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int finalization_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string run_type { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string cron_expression { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string extract_mode { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string upload_mode { get; set; }
        [Column(TypeName = "text")]
        public string key_field { get; set; }
        [Column(TypeName = "text")]
        public string extract_condition { get; set; }
        [Column(TypeName = "int")]
        public int pull_days { get; set; }
        [Column(TypeName = "char(1)")]
        public string reject_duplicate_flag { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string error_mode { get; set; }
        [Column(TypeName = "varchar(255)")]
        public string last_incremental_val { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class PipelineIncrementalRecord
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int incremental_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string incremental_field { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string incremental_value { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class DataSet
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int dataset_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string dataset_code { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string dataset_name { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string module_code { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string table_name { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string connector_code { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    [Keyless]
    public class DataSetField
    {
        public int dataset_field_gid { get; set; }
        public string dataset_code { get; set; }
        public string dataset_field_desc { get; set; }
        public string dataset_field_name { get; set; }
        public string dataset_field_type { get; set; }
        public string dataset_table_field { get; set; }
        public string field_mandatory { get; set; }
        public string active_status { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
    }

    public class DataSetImport
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int dataset_import_gid { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipelinecode { get; set; }
        [Column(TypeName = "text")]
        public string dataset_info { get; set; }
        [Column(TypeName = "int(11)")]
        public int job_gid { get; set; }
        [Column(TypeName = "varchar(64)")]
        public string status_code { get; set; }
        [Column(TypeName = "text")]
        public string remarks { get; set; }
        public DateTime created_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class Scheduler
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int scheduler_gid { get; set; }
        [Column(TypeName = "datetime")]
        public DateTime scheduled_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string pipeline_code { get; set; }
        [Column(TypeName = "text")]
        public string scheduler_parameters { get; set; }
        [Column(TypeName = "text")]
        public string file_path { get; set; }
        [Column(TypeName = "varchar(128)")]
        public string file_name { get; set; }
        public DateTime scheduler_start_date { get; set; }
        public DateTime scheduler_end_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string scheduler_status { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string additional_status { get; set; } = "NA";
        [Column(TypeName = "text")]
        public string scheduler_remark { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string scheduler_initiated_by { get; set; }
        [Column(TypeName = "datetime")]
        public DateTime last_update_date { get; set; }
        [Column(TypeName = "char(1)")]
        public string delete_flag { get; set; }
    }

    public class Master
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public Int64 master_gid { get; set; }

        [Column(TypeName = "varchar(50)")]
        [Required]
        public string master_code { get; set; }
        [Column(TypeName = "varchar(255)")]
        [Required]
        public string master_name { get; set; }

        [Column(TypeName = "bigint(20)")]
        [NullValues]
        public Int64 parent_gid { get; set; } = 0;
        [Column(TypeName = "varchar(50)")]
        [NullValues]
        public string parent_code { get; set; }
        [Column(TypeName = "bigint(20)")]
        [NullValues]
        public Int64 depend_gid { get; set; } = 0;
        [Column(TypeName = "varchar(50)")]
        [NullValues]
        public string depend_code { get; set; }
        [Required]
        public DateTime created_date { get; set; } = DateTime.Now;
        [Required]
        [Column(TypeName = "varchar(32)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(32)")]
        public string? updated_by { get; set; } = null;
        [Column(TypeName = "char(1)")]
        [Required]
        public char delete_flag { get; set; } = 'N';
    }

    public class dataProcessing
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public Int64 dataprocessing_gid { get; set; }

        [Column(TypeName = "int(11)")]
        [Required]
        public Int64 dataprocessing_header_gid { get; set; }

        [Column(TypeName = "bigint")]
        [NullValues]
        public Int64 dataprocessing_child_master_gid { get; set; } = 0;

        [Column(TypeName = "bigint")]
        [NullValues]
        public Int64 dataprocessing_parent_master_gid { get; set; } = 0;

        [Column(TypeName = "bigint")]
        [Required]
        public Int64 dataprocessing_master_gid { get; set; }

        [Column(TypeName = "int(11)")]
        [Required]
        public int dataprocessing_pipeline_gid { get; set; }

        [Column(TypeName = "int(11)")]
        [Required]
        public int dataprocessing_pplfieldmapping_gid { get; set; }

        [Column(TypeName = "int(11)")]
        [Required]
        public int dataprocessing_orderby { get; set; }

        [Required]
        [Column(TypeName = "varchar(50)")]
        public string dataprocessing_param1 { get; set; }

        [NullValues]
        [Column(TypeName = "varchar(50)")]
        public string dataprocessing_param2 { get; set; } = null;

        [NullValues]
        [Column(TypeName = "varchar(50)")]
        public string dataprocessing_param3 { get; set; } = null;

        [Required]
        public DateTime created_date { get; set; } = DateTime.Now;
        [Required]
        [Column(TypeName = "varchar(50)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(50)")]
        public string? updated_by { get; set; } = null;
        [Column(TypeName = "char(1)")]
        [Required]
        public char delete_flag { get; set; } = 'N';

        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_ppl_field_name { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_master_name { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_parent_master_name { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_child_master_name { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_master_code { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_parent_master_code { get; internal set; } = null;
        [NotMapped] // This field will not be mapped to a database column
        public string dataprocessing_child_master_code { get; internal set; } = null;
    }

    public class dataProcessingheader
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public Int64 dataprocessingheader_gid { get; set; }

        [Column(TypeName = "varchar(50)")]
        [Required]
        public string dataprocessingheader_pipeline_code { get; set; }

        [Column(TypeName = "bigint(20)")]
        [Required]
        public Int64 dataprocessingheader_pplfieldmapping_gid { get; set; }

        [Column(TypeName = "int(11)")]
        [Required]
        public int dataprocessingheader_seqno { get; set; }

        [Column(TypeName = "varchar(128)")]
        [Required]
        public string dataprocessingheader_ppl_field_name { get; set; }

        [Required]
        public DateTime created_date { get; set; } = DateTime.Now;

        [Required]
        [Column(TypeName = "varchar(50)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(50)")]
        public string? updated_by { get; set; } = null;
        [Column(TypeName = "char(1)")]
        [Required]
        public char delete_flag { get; set; } = 'N';
    }
    public class fieldConfig
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public Int64 fieldconfig_gid { get; set; }

        [Column(TypeName = "varchar(50)")]
        [Required]
        public string fieldconfig_master_code { get; set; }

        [Column(TypeName = "char(1)")]
        [Required]
        public string fieldconfig_expressions { get; set; }

        [Column(TypeName = "varchar(150)")]
        public string fieldconfig_expression_label { get; set; }

        [Column(TypeName = "JSON")]
        [Required]
        public string fieldconfig_dynamicfields { get; set; }

        [Required]
        public DateTime created_date { get; set; } = DateTime.Now;

        [Required]
        [Column(TypeName = "varchar(50)")]
        public string created_by { get; set; }
        public DateTime? updated_date { get; set; }
        [Column(TypeName = "varchar(50)")]
        public string? updated_by { get; set; } = null;

        [Column(TypeName = "char(1)")]
        [Required]
        public char delete_flag { get; set; } = 'N';
    }



    [Keyless]
    public class columnDatatype
    {
        public string pipeline_code { get; set; }
        public string dataset_code { get; set; }
        public string dataset_field_type { get; set; }
        public string sourcefield_name { get; set; }
        public string dataset_field_name { get; set; }
        public string source_type { get; set; }
        
    }

    [Keyless]
    public class Config
    {
        public string config_name { get; set; }
        public string config_value { get; set; }
        public string delete_flag { get; set; }
    }
    #endregion

    #region Declaration
    public class Sheet
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    public class ConnectionDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class DatabaseInfo
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    public class SrcExpression
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
    public class TableAndView
    {
        public string Name { get; set; }
    }
    public class SourcetblFields
    {
        public string ID { get; set; }
        public string Name { get; set; }
    }
    public class TargettblKeyfields
    {
        public string ID { get; set; }
        public string Name { get; set; }
    }

    public class TargetTable
    {
        public string dataset_code { get; set; }
        public string dataset_name { get; set; }
    }
    public class TableFields
    {
        public string source_field_name { get; set; }
        public string target_field_name { get; set; }
    }

    public class NewScheduler
    {
        public string pipeline_code { get; set; }
        public IFormFile file { get; set; }
        public string initiated_by { get; set; }
    }

    public class NewSchedulerForothers
    {
        public int scheduler_gid { get; set; }
        public string pipeline_code { get; set; }
        public string initiated_by { get; set; }
    }

    public class validateexcelsheet
    {
        public string file_Paths { get; set; }
        public string sheetName { get; set; }
    }

    public class pipelineclone
    {
        public string in_pipeline_name { get; set; }
        public string in_pipeline_code { get; set; }
        public string in_dataset_code { get; set; }
        public string out_srcfile_name { get; set; }
        public string out_dstfile_name { get; set; }
        public string out_msg { get; set; }
        public int out_result { get; set; }
    }

    public class ErrorLog
    {
        public string in_errorlog_pipeline_code { get; set; }
        public int in_errorlog_scheduler_gid { get; set; }
        public string in_errorlog_type { get; set; }
        public string in_errorlog_exception { get; set; }
        public string in_created_by { get; set; }
    }

    public class PipelinecloneResult
    {
        public string SrcFileName { get; set; }
        public string DstFileName { get; set; }
        public string Message { get; set; }
        public int Result { get; set; }
    }
    #endregion


    public class getVallistModel
    {
        public string in_pipeline_code { get; set; }
    }

}
