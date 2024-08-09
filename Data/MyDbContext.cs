using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MysqlEfCoreDemo.Models;


namespace MysqlEfCoreDemo.Data
{
    public class MyDbContext : DbContext
    {
        public MyDbContext(DbContextOptions<MyDbContext> options) : base(options)
        {

        }
        public DbSet<ConnectionModel> con_mst_tconnection { get; set; }
        public DbSet<Pipeline> con_mst_tpipeline { get; set; }
        public DbSet<DataSet> con_mst_tdataset { get; set; }
        public DbSet<DataSetField> con_mst_tdataset_field { get; set; }
        public DbSet<PipelineSourcefield> con_trn_tpplsourcefield { get; set; }
        public DbSet<PipelineMapping> con_trn_tpplfieldmapping { get; set; }
        public DbSet<PipelineCondition> con_trn_tpplcondition { get; set; }
        //public DbSet<PipelineCondition> con_trn_tincrementalrecord { get; set; }
        public DbSet<PipelineFinalization> con_trn_tpplfinalization { get; set; }
        public DbSet<PipelineIncrementalRecord> con_trn_tincrementalrecord { get; set; }
        public DbSet<DataSetImport> con_trn_tdatasetimport { get; set; }
        public DbSet<Scheduler> con_trn_tscheduler { get; set; }
        public DbSet<Master> con_mst_tmaster { get; set; }
        public DbSet<dataProcessing> con_mst_tdataprocessing { get; set; }
        public DbSet<dataProcessingheader> con_mst_tdataprocessingheader { get; set; }
        public DbSet<fieldConfig> con_mst_tfieldconfig { get; set; }
        public DbSet<columnDatatype> con_col_datatype { get; set; }
        public DbSet<Config> con_mst_tconfig { get; set; }


    }


}