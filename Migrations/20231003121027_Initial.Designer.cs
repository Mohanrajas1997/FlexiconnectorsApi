﻿// <auto-generated />
using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using MysqlEfCoreDemo.Data;

#nullable disable

namespace MysqlEfCoreDemo.Migrations
{
    [DbContext(typeof(MyDbContext))]
    [Migration("20231003121027_Initial")]
    partial class Initial
    {
        /// <inheritdoc />
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("ProductVersion", "7.0.9")
                .HasAnnotation("Relational:MaxIdentifierLength", 64);

            modelBuilder.Entity("MysqlEfCoreDemo.Models.Connection", b =>
                {
                    b.Property<int>("connection_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("connection_code")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("connection_desc")
                        .HasColumnType("text");

                    b.Property<string>("connection_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("connection_status")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("file_password")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("protection_type")
                        .HasColumnType("char(1)");

                    b.Property<byte[]>("source_auth_file_blob")
                        .HasColumnType("blob");

                    b.Property<string>("source_auth_file_name")
                        .HasColumnType("text");

                    b.Property<string>("source_auth_mode")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("source_db_pwd")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("source_db_type")
                        .HasColumnType("varchar(16)");

                    b.Property<string>("source_db_user")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("source_file")
                        .HasColumnType("text");

                    b.Property<string>("source_host_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("source_port")
                        .HasColumnType("varchar(8)");

                    b.Property<string>("ssh_auth_mode")
                        .HasColumnType("varchar(128)");

                    b.Property<byte[]>("ssh_file_blob")
                        .HasColumnType("blob");

                    b.Property<string>("ssh_file_name")
                        .HasColumnType("text");

                    b.Property<string>("ssh_host_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("ssh_port")
                        .HasColumnType("varchar(8)");

                    b.Property<string>("ssh_pwd")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("ssh_tunneling")
                        .HasColumnType("char(1)");

                    b.Property<string>("ssh_user")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("connection_gid");

                    b.ToTable("con_mst_tconnection");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.DataSet", b =>
                {
                    b.Property<int>("dataset_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("connector_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("dataset_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("dataset_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("module_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("table_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("dataset_gid");

                    b.ToTable("con_mst_tdataset");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.DataSetField", b =>
                {
                    b.Property<int>("dataset_field_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("dataset_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("dataset_field_desc")
                        .HasColumnType("varchar(256)");

                    b.Property<string>("dataset_field_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("dataset_field_type")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("dataset_field_gid");

                    b.ToTable("con_mst_tdataset_field");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.DataSetImport", b =>
                {
                    b.Property<int>("dataset_import_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("dataset_info")
                        .HasColumnType("text");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<int>("job_gid")
                        .HasColumnType("int(11)");

                    b.Property<string>("pipelinecode")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("remarks")
                        .HasColumnType("text");

                    b.Property<string>("status_code")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("dataset_import_gid");

                    b.ToTable("con_trn_tdatasetimport");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.Pipeline", b =>
                {
                    b.Property<int>("pipeline_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("connection_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("db_name")
                        .HasColumnType("varchar(256)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("pipeline_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("pipeline_desc")
                        .HasColumnType("text");

                    b.Property<string>("pipeline_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("pipeline_status")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("sheet_name")
                        .HasColumnType("varchar(256)");

                    b.Property<string>("source_file_name")
                        .HasColumnType("varchar(256)");

                    b.Property<string>("table_view_query_desc")
                        .HasColumnType("text");

                    b.Property<string>("table_view_query_type")
                        .HasColumnType("char(1)");

                    b.Property<string>("target_dataset_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("pipeline_gid");

                    b.ToTable("con_mst_tpipeline");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.PipelineCondition", b =>
                {
                    b.Property<int>("pplcondition_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("default_condition")
                        .HasColumnType("text");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("key_field")
                        .HasColumnType("text");

                    b.Property<string>("pipeline_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("pplcondition_gid");

                    b.ToTable("con_trn_tpplcondition");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.PipelineFinalization", b =>
                {
                    b.Property<int>("finalization_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("cron_expression")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("extract_mode")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("pipeline_code")
                        .HasColumnType("varchar(32)");

                    b.Property<int>("pull_days")
                        .HasColumnType("int");

                    b.Property<string>("run_type")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("updated_time_stamp")
                        .HasColumnType("varchar(64)");

                    b.Property<string>("upload_mode")
                        .HasColumnType("varchar(64)");

                    b.HasKey("finalization_gid");

                    b.ToTable("con_trn_tpplfinalization");
                });

            modelBuilder.Entity("MysqlEfCoreDemo.Models.PipelineMapping", b =>
                {
                    b.Property<int>("pplfieldmapping_gid")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int");

                    b.Property<string>("created_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime>("created_date")
                        .HasColumnType("datetime(6)");

                    b.Property<string>("dataset_field_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("default_value")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("delete_flag")
                        .HasColumnType("char(1)");

                    b.Property<string>("pipeline_code")
                        .HasColumnType("varchar(32)");

                    b.Property<string>("ppl_field_name")
                        .HasColumnType("varchar(128)");

                    b.Property<string>("updated_by")
                        .HasColumnType("varchar(32)");

                    b.Property<DateTime?>("updated_date")
                        .HasColumnType("datetime(6)");

                    b.HasKey("pplfieldmapping_gid");

                    b.ToTable("con_trn_tpplfieldmapping");
                });
#pragma warning restore 612, 618
        }
    }
}
