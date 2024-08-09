using System;
using Microsoft.EntityFrameworkCore.Migrations;
using MySql.EntityFrameworkCore.Metadata;

#nullable disable

namespace MysqlEfCoreDemo.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tconnection",
                columns: table => new
                {
                    connection_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    connection_code = table.Column<string>(type: "varchar(64)", nullable: true),
                    connection_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    connection_desc = table.Column<string>(type: "text", nullable: true),
                    source_db_type = table.Column<string>(type: "varchar(16)", nullable: true),
                    protection_type = table.Column<string>(type: "char(1)", nullable: true),
                    file_password = table.Column<string>(type: "varchar(64)", nullable: true),
                    source_host_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    source_port = table.Column<string>(type: "varchar(8)", nullable: true),
                    source_auth_mode = table.Column<string>(type: "varchar(32)", nullable: true),
                    source_db_user = table.Column<string>(type: "varchar(128)", nullable: true),
                    source_db_pwd = table.Column<string>(type: "varchar(64)", nullable: true),
                    source_auth_file_name = table.Column<string>(type: "text", nullable: true),
                    source_auth_file_blob = table.Column<byte[]>(type: "blob", nullable: true),
                    source_file = table.Column<string>(type: "text", nullable: true),
                    ssh_tunneling = table.Column<string>(type: "char(1)", nullable: true),
                    ssh_host_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_port = table.Column<string>(type: "varchar(8)", nullable: true),
                    ssh_user = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_pwd = table.Column<string>(type: "varchar(64)", nullable: true),
                    ssh_auth_mode = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_file_name = table.Column<string>(type: "text", nullable: true),
                    ssh_file_blob = table.Column<byte[]>(type: "blob", nullable: true),
                    connection_status = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tconnection", x => x.connection_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataset",
                columns: table => new
                {
                    dataset_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    module_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    table_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    connector_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tdataset", x => x.dataset_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataset_field",
                columns: table => new
                {
                    dataset_field_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_field_desc = table.Column<string>(type: "varchar(256)", nullable: true),
                    dataset_field_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    dataset_field_type = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tdataset_field", x => x.dataset_field_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tpipeline",
                columns: table => new
                {
                    pipeline_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    pipeline_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    pipeline_desc = table.Column<string>(type: "text", nullable: true),
                    connection_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    db_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    source_file_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    sheet_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    table_view_query_type = table.Column<string>(type: "char(1)", nullable: true),
                    table_view_query_desc = table.Column<string>(type: "text", nullable: true),
                    target_dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    pipeline_status = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tpipeline", x => x.pipeline_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tdatasetimport",
                columns: table => new
                {
                    dataset_import_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipelinecode = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_info = table.Column<string>(type: "text", nullable: true),
                    job_gid = table.Column<int>(type: "int(11)", nullable: false),
                    status_code = table.Column<string>(type: "varchar(64)", nullable: true),
                    remarks = table.Column<string>(type: "text", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tdatasetimport", x => x.dataset_import_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplcondition",
                columns: table => new
                {
                    pplcondition_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    key_field = table.Column<string>(type: "text", nullable: true),
                    default_condition = table.Column<string>(type: "text", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplcondition", x => x.pplcondition_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplfieldmapping",
                columns: table => new
                {
                    pplfieldmapping_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    ppl_field_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    dataset_field_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    default_value = table.Column<string>(type: "varchar(128)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplfieldmapping", x => x.pplfieldmapping_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplfinalization",
                columns: table => new
                {
                    finalization_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    run_type = table.Column<string>(type: "varchar(64)", nullable: true),
                    cron_expression = table.Column<string>(type: "varchar(64)", nullable: true),
                    extract_mode = table.Column<string>(type: "varchar(64)", nullable: true),
                    updated_time_stamp = table.Column<string>(type: "varchar(64)", nullable: true),
                    pull_days = table.Column<int>(type: "int", nullable: false),
                    upload_mode = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplfinalization", x => x.finalization_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "con_mst_tconnection");

            migrationBuilder.DropTable(
                name: "con_mst_tdataset");

            migrationBuilder.DropTable(
                name: "con_mst_tdataset_field");

            migrationBuilder.DropTable(
                name: "con_mst_tpipeline");

            migrationBuilder.DropTable(
                name: "con_trn_tdatasetimport");

            migrationBuilder.DropTable(
                name: "con_trn_tpplcondition");

            migrationBuilder.DropTable(
                name: "con_trn_tpplfieldmapping");

            migrationBuilder.DropTable(
                name: "con_trn_tpplfinalization");
        }
    }
}
