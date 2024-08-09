using ClosedXML.Excel;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using MysqlEfCoreDemo.Data;
using MysqlEfCoreDemo.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DataSet = System.Data.DataSet;
using Match = System.Text.RegularExpressions.Match;

namespace MysqlEfCoreDemo.Controllers
{
    public class PipelineController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly MyDbContext dbContext;

        #region Global variables
        string conn = "";
        string trg_hstname = "";
        string trg_dbname = "";
        string trg_username = "";
        string trg_password = "";
        string csvfilePath = "";
        string targetconnectionString = "";
        string errorlogfilePath = ""; //"D:\\Mohan\\error_log.txt";
        string dbtype = "";
        string errormsg = "";
        string uploadfilepath = "";
        string clonefilepath = "";
        int sched_gid = 0;
        string strtodate_format;
        string strtodatetime_format;
        string src_filename = "";
        string v_filepath = ""; //"D:\\Mohan\\ExcelScheduler\\";
        string comp_file_path = "";
        string hostingfor = "";
        string _slash = "";
        string lineterm = "\r\n";
        string initiated_by = "";
        string ppl_code = "";
        #endregion

        public PipelineController(MyDbContext dbContext, IConfiguration configuration)
        {
            _configuration = configuration;
            hostingfor = _configuration["HostingFor"];// _configuration.GetConnectionString("HostingFor");
            if (hostingfor.Trim() == "Linux")
            {
                _slash = "/";
                lineterm = "\n";
            }
            else
            {
                _slash = "\\";
            }
            targetconnectionString = _configuration.GetConnectionString("targetMysql");
            conn = _configuration["conn"];
            trg_hstname = _configuration["trg_hstname"];
            trg_dbname = _configuration["trg_dbname"];
            trg_username = _configuration["trg_username"];
            trg_password = _configuration["trg_password"];
            csvfilePath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Processing") + _slash; //+ "Processing.csv"; //_configuration["csvfilePath"]; //WINDOWS Server
            uploadfilepath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "RawFiles") + _slash; //_configuration["uploadfilepath"];
            clonefilepath = _configuration["clonefilepath"];//System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Uploads") + _slash; //_configuration["clonefilepath"];
            v_filepath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "ExcelScheduler") + _slash; //"D:\\Mohan\\ExcelScheduler\\";
            errorlogfilePath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Errorlog") + _slash + "error_log.txt";
            comp_file_path = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "CompletedFiles") + _slash; //"D:\\Mohan\\CompletedFiles\\";
            dbtype = _configuration["trgdbtype"];
            strtodate_format = _configuration["str_to_date_format"];
            strtodatetime_format = _configuration["str_to_datetime_format"];
            this.dbContext = dbContext;

        }

        // Implement IDisposable interface
        public void Dispose()
        {
            dbContext.Dispose();
        }

        #region Pipeline Header
        [HttpGet]
        public async Task<IActionResult> GetPipelines(string runtype, string pipeline_status)
        {
            try
            {

                var result = await dbContext.con_mst_tpipeline
                        .GroupJoin(
                            dbContext.con_mst_tdataset,
                            a => a.target_dataset_code,
                            b => b.dataset_code,
                            (a, bGroup) => new { EntityA = a, EntityBGroup = bGroup }
                        )
                        .SelectMany(
                            ab => ab.EntityBGroup.DefaultIfEmpty(),
                            (ab, b) => new { ab.EntityA, EntityB = b }
                        )
                        .GroupJoin(
                            dbContext.con_trn_tpplfieldmapping,
                            ab => ab.EntityA.pipeline_code,
                            c => c.pipeline_code,
                            (ab, cGroup) => new { ab.EntityA, ab.EntityB, EntityCGroup = cGroup }
                        )
                        .SelectMany(
                            abc => abc.EntityCGroup.DefaultIfEmpty(),
                            (abc, c) => new
                            {
                                EntityA = abc.EntityA,
                                EntityB = abc.EntityB,
                                EntityC = c
                            }
                        )
                        .GroupJoin(
                            dbContext.con_trn_tpplcondition,
                            abc => abc.EntityA.pipeline_code,
                            d => d.pipeline_code,
                            (abc, dGroup) => new { abc.EntityA, abc.EntityB, abc.EntityC, EntityDGroup = dGroup }
                        )
                        .SelectMany(
                            abcd => abcd.EntityDGroup.DefaultIfEmpty(),
                            (abcd, d) => new
                            {
                                EntityA = abcd.EntityA,
                                EntityB = abcd.EntityB,
                                EntityC = abcd.EntityC,
                                EntityD = d
                            }
                        )
                        .GroupJoin(
                            dbContext.con_trn_tpplfinalization,
                            abcd => abcd.EntityA.pipeline_code,
                            e => e.pipeline_code,
                            (abcd, eGroup) => new { abcd.EntityA, abcd.EntityB, abcd.EntityC, abcd.EntityD, EntityEGroup = eGroup }
                        )
                        .SelectMany(
                            abcde => abcde.EntityEGroup.DefaultIfEmpty(),
                            (abcde, e) => new
                            {
                                EntityA = abcde.EntityA,
                                EntityB = abcde.EntityB,
                                EntityC = abcde.EntityC,
                                EntityD = abcde.EntityD,
                                EntityE = e
                            }
                        )
                        .Join(
                            dbContext.con_mst_tconnection,
                            abcde => abcde.EntityA.connection_code,
                            conn => conn.connection_code,
                            (abcde, conn) => new
                            {
                                EntityA = abcde.EntityA,
                                EntityB = abcde.EntityB,
                                EntityC = abcde.EntityC,
                                EntityD = abcde.EntityD,
                                EntityE = abcde.EntityE,
                                Connection = conn
                            }
                        )
                        .Select(result => new
                        {
                            result.EntityA.pipeline_gid,
                            result.EntityA.pipeline_code,
                            result.EntityA.pipeline_name,
                            result.EntityA.connection_code,
                            result.EntityA.db_name,
                            result.EntityA.table_view_query_type,
                            result.EntityA.table_view_query_desc,
                            result.EntityA.target_dataset_code,
                            result.EntityA.delete_flag,
                            result.EntityA.created_date,
                            result.EntityA.created_by,
                            result.EntityA.updated_date,
                            result.EntityA.updated_by,
                            result.EntityA.pipeline_status,
                            result.EntityB.dataset_name,
                            result.EntityB.table_name,
                            result.EntityE.run_type,
                            result.EntityE.upload_mode,
                            //result.EntityE.updated_time_stamp,
                            result.EntityE.pull_days,
                            result.EntityC.ppl_field_name,
                            result.EntityC.default_value,
                            result.Connection, // include connection information
                        })
                        .GroupBy(item => item.pipeline_code) // Group by pipeline_code
                      .Select(group => new
                      {
                          pipeline_code = group.Key,

                          pipeline_gid = group.Select(item => item.pipeline_gid).FirstOrDefault(),
                          pipeline_name = group.Select(item => item.pipeline_name).FirstOrDefault(),
                          connection_code = group.Select(item => item.connection_code).FirstOrDefault(),
                          db_name = group.Select(item => item.db_name).FirstOrDefault(),
                          table_view_query_type = group.Select(item => item.table_view_query_type).FirstOrDefault(),
                          target_dataset_code = group.Select(item => item.target_dataset_code).FirstOrDefault(),
                          table_view_query_desc = group.Select(item => item.table_view_query_desc).FirstOrDefault(),
                          delete_flag = group.Select(item => item.delete_flag).FirstOrDefault(),
                          created_date = group.Select(item => item.created_date).FirstOrDefault(),
                          created_by = group.Select(item => item.created_by).FirstOrDefault(),
                          updated_date = group.Select(item => item.updated_date).FirstOrDefault(),
                          updated_by = group.Select(item => item.updated_by).FirstOrDefault(),
                          pipeline_status = group.Select(item => item.pipeline_status).FirstOrDefault(),
                          table_name = group.Select(item => item.table_name).FirstOrDefault(),
                          dataset_name = group.Select(item => item.dataset_name).FirstOrDefault(),
                          PplFieldNames = string.Join(", ", group.Select(item => item.ppl_field_name).Where(fieldName => fieldName != null)),
                          //key_field = group.Select(item => item.key_field).FirstOrDefault(),
                          default_value = string.Join(", ", group.Select(item => item.default_value).Where(defaultValue => defaultValue != null)),
                          run_type = group.Select(item => item.run_type).FirstOrDefault(),
                          upload_mode = group.Select(item => item.upload_mode).FirstOrDefault(),
                          //updated_time_stamp = group.Select(item => item.updated_time_stamp).FirstOrDefault(),
                          pull_days = group.Select(item => item.pull_days).FirstOrDefault(),
                          source_db_type = group.Select(item => item.Connection.source_db_type).FirstOrDefault(),
                          connector_name = group.Select(item => item.Connection.connection_name).FirstOrDefault()
                      })
                      .Where(item => (string.IsNullOrEmpty(runtype) || item.run_type == runtype) && (string.IsNullOrEmpty(pipeline_status) || item.pipeline_status == pipeline_status) && item.delete_flag == "N")
                      .OrderByDescending(item => item.pipeline_gid)
                      .ToListAsync();

                return Ok(result);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetPipeline(int id)
        {

            var pipeline = await dbContext.con_mst_tpipeline.FindAsync(id);

            try
            {
                if (pipeline == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(pipeline);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetConnectorsForDropdown()
        {
            try
            {
                var Lovconn = dbContext.con_mst_tconnection
                .Where(p => p.connection_status == "Active")
                .Select(c => new ConnectionDto
                {
                    Id = c.connection_code,
                    Name = c.connection_name
                })
                .ToList();

                return Ok(Lovconn);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }
        [HttpGet]
        public async Task<IActionResult> GetSourcedbType(string connection_code)
        {
            try
            {
                var res = dbContext.con_mst_tconnection
                   .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                   .Select(p => new ConnectionModel
                   {
                       source_db_type = p.source_db_type,
                   })
                   .SingleOrDefault();

                return Ok(res);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<string> GetSourcedbType_pplcode(string pipeline_code)
        {
            string src_dbtype = "";
            try
            {
                var ppl = dbContext.con_mst_tpipeline
                   .Where(p => p.pipeline_code == pipeline_code && p.delete_flag == "N")
                   .Select(p => new ConnectionModel
                   {
                       connection_code = p.connection_code,
                   })
                   .SingleOrDefault();

                var res = dbContext.con_mst_tconnection
                   .Where(p => p.connection_code == ppl.connection_code && p.delete_flag == "N")
                   .Select(p => new ConnectionModel
                   {
                       source_db_type = p.source_db_type,
                   })
                   .SingleOrDefault();

                src_dbtype = res.source_db_type;
                return src_dbtype;

            }
            catch (Exception ex)
            {
                src_dbtype = $"Error: {ex.Message}";
                return src_dbtype;
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetDatabaseNames(string connection_code)
        {
            var connector = dbContext.con_mst_tconnection
                 .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                 .Select(p => new ConnectionModel
                 {
                     source_host_name = p.source_host_name,
                     source_port = p.source_port,
                     source_db_user = p.source_db_user,
                     source_db_pwd = p.source_db_pwd,
                     source_db_type = p.source_db_type,
                 })
                 .SingleOrDefault();
            try
            {
                if (connector == null)

                {
                    return NotFound("No Data found");
                }
                var connstring = "";
                List<DatabaseInfo> databaseNames = new List<DatabaseInfo>();

                if (connector.source_db_type == "MySql")
                {
                    connstring = "server=" + connector.source_host_name + "; uid=" +
                                connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";
                    using (MySqlConnection connection = new MySqlConnection(connstring))
                    {
                        connection.Open();

                        MySqlCommand command = connection.CreateCommand();
                        command.CommandText = "SHOW DATABASES";
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string databaseName = reader.GetString(0);
                                if (!IsSystemDatabase(databaseName))
                                {
                                    databaseNames.Add(new DatabaseInfo { Name = databaseName });
                                }
                            }
                        }
                        connection.Close();
                    }
                }

                else if (connector.source_db_type == "Postgres")
                {
                    connstring = "Host=" + connector.source_host_name + "; Database=postgres; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";

                    using (NpgsqlConnection connection = new NpgsqlConnection(connstring))
                    {
                        connection.Open();

                        NpgsqlCommand command = connection.CreateCommand();
                        command.CommandText = "SELECT datname as DATABASES FROM pg_database;";
                        using (NpgsqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string databaseName = reader.GetString(0);
                                if (!IsSystemDatabase(databaseName))
                                {
                                    databaseNames.Add(new DatabaseInfo { Name = databaseName });
                                }
                            }
                        }
                        connection.Close();
                    }
                }

                return Ok(databaseNames);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public List<DatabaseInfo> GetExcelSheetNames(string filePath, string password)
        {
            List<DatabaseInfo> sheetNames = new List<DatabaseInfo>();

            try
            {
                using (var workbook = new XLWorkbook(filePath))
                {
                    foreach (var sheet in workbook.Worksheets)
                    {
                        // Assuming you want to skip certain sheets like "Print_Area"
                        if (!sheet.Name.EndsWith("Print_Area"))
                        {
                            sheetNames.Add(new DatabaseInfo { Name = sheet.Name });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                // Handle exception
            }

            return sheetNames;
        }

        [HttpGet]
        public List<DatabaseInfo> GetExcelSheetNames_OLEDB(string filePath, string password)
        {
            List<DatabaseInfo> sheetNames = new List<DatabaseInfo>();
            string[] lastIndex = filePath.Split(".");
            string fileExtension = lastIndex[1];

            try
            {
                string connectionString = "";// @"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + @filePath + ";Extended Properties='Excel 12.0;HDR=YES;'";

                if (fileExtension == "xls")
                {
                    connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath + ";Extended Properties=\"Excel 8.0;HDR=Yes;IMEX=1\"";
                }
                else if (fileExtension == "xlsx")
                {
                    connectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath + ";Extended Properties=\"Excel 12.0;HDR=Yes;IMEX=1\"";
                }


                using (OleDbConnection connection = new OleDbConnection(connectionString))
                {
                    connection.Open();
                    DataTable schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);

                    if (schema != null)
                    {
                        foreach (DataRow row in schema.Rows)
                        {
                            string sheetName = row["TABLE_NAME"].ToString();

                            if (sheetName.EndsWith("$") && !sheetName.EndsWith("Print_Area$"))
                            {
                                // Trim and clean up the sheet name
                                sheetName = sheetName.Trim('\'', '$');

                                // Create a new DatabaseInfo object and add it to the list
                                sheetNames.Add(new DatabaseInfo { Name = sheetName });
                            }
                        }
                    }
                    //connection.Close();
                    //connection.Dispose();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

                //logger.Error("Step 5 : " + ex.Message);
            }

            return sheetNames;
        }

        [HttpGet]
        public async Task<List<DatabaseInfo>> ReadFirstRowFromExcel(string pipelinecode, string filePath, string sheetName, string user_code)
        {
            List<DatabaseInfo> Excelsrcfieldname = new List<DatabaseInfo>();

            try
            {
                // Check the file extension to determine the format.
                string fileExtension = System.IO.Path.GetExtension(filePath);
                AddpplSourceFieldRequest objsrcfld = new AddpplSourceFieldRequest();

                // Delete the previous source field against pipelinecode
                var pplsrcFieldsToDelete = await dbContext.con_trn_tpplsourcefield
                .Where(p => p.pipeline_code == pipelinecode)//&& p.source_type != "Expression")
                .ToListAsync();

                if (pplsrcFieldsToDelete.Any())
                {
                    dbContext.con_trn_tpplsourcefield.RemoveRange(pplsrcFieldsToDelete);
                    await dbContext.SaveChangesAsync();
                }

                int i = 1;

                if (string.Equals(fileExtension, ".xls", StringComparison.OrdinalIgnoreCase))
                {
                    // Read .xls (Excel 97-2003) file using NPOI
                    using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                    {
                        IWorkbook workbook = new HSSFWorkbook(fileStream);
                        ISheet worksheet = workbook.GetSheet(sheetName);

                        if (worksheet != null)
                        {
                            // Assuming you want to read data from the first row (row 1)
                            IRow row = worksheet.GetRow(0);

                            if (row != null)
                            {

                                // Loop through the cells in the row
                                foreach (ICell cell in row.Cells)
                                {
                                    Excelsrcfieldname.Add(new DatabaseInfo { Name = cell.StringCellValue });

                                    var pplsrcfld = new PipelineSourcefield()
                                    {
                                        pplsourcefield_gid = 0,
                                        pipeline_code = pipelinecode,
                                        sourcefield_name = cell.StringCellValue,
                                        sourcefield_sno = i,
                                        dataset_table_field = "",//"col" + i,
                                        expressionfield_json = null,
                                        sourcefieldmapping_flag = "N",
                                        source_type = "Excel",
                                        created_by = user_code,
                                        created_date = DateTime.Now,
                                        delete_flag = "N"
                                    };
                                    dbContext.con_trn_tpplsourcefield.Add(pplsrcfld);
                                    i++;
                                }
                            }
                        }
                    }
                }
                else if (string.Equals(fileExtension, ".xlsx", StringComparison.OrdinalIgnoreCase))
                {
                    using (var workbook = new XLWorkbook(filePath))
                    {
                        var worksheet = workbook.Worksheet(sheetName);

                        if (worksheet != null)
                        {
                            // Assuming you want to read data from the first row (row 1)
                            var row = worksheet.Row(1).Cells(1, worksheet.LastColumnUsed().ColumnNumber());

                            // Loop through the cells in the row
                            foreach (var cell in row)
                            {
                                Excelsrcfieldname.Add(new DatabaseInfo { Name = cell.GetValue<string>() });
                                var pplsrcfld = new PipelineSourcefield()
                                {
                                    pplsourcefield_gid = 0,
                                    pipeline_code = pipelinecode,
                                    sourcefield_name = cell.GetValue<string>(),
                                    sourcefield_sno = i,
                                    dataset_table_field = "",//"col" + i,
                                    expressionfield_json = null,
                                    sourcefieldmapping_flag = "N",
                                    source_type = "Excel",
                                    created_by = user_code,
                                    created_date = DateTime.Now,
                                    delete_flag = "N"
                                };
                                dbContext.con_trn_tpplsourcefield.Add(pplsrcfld);
                                i++;
                            }
                        }
                    }
                }

                // Save all changes to the database
                await dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                // Handle any exceptions here.
                Console.WriteLine("Error: " + ex.Message);
            }
            return Excelsrcfieldname;
        }

        [HttpGet]
        public async Task<List<DatabaseInfo>> ReadTablecolFromSource(string connection_code, string pipelinecode,
            string databasename, string sourcetable, string tvq_type, string user_code)
        {
            List<DatabaseInfo> srcfieldname = new List<DatabaseInfo>();

            try
            {
                // Delete the previous source field against pipelinecode
                var pplsrcFieldsToDelete = await dbContext.con_trn_tpplsourcefield
                .Where(p => p.pipeline_code == pipelinecode)
                .ToListAsync();

                if (pplsrcFieldsToDelete.Any())
                {
                    dbContext.con_trn_tpplsourcefield.RemoveRange(pplsrcFieldsToDelete);
                    await dbContext.SaveChangesAsync();
                }


                var connector = dbContext.con_mst_tconnection
               .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
               .Select(p => new ConnectionModel
               {
                   source_host_name = p.source_host_name,
                   source_port = p.source_port,
                   source_db_user = p.source_db_user,
                   source_db_pwd = p.source_db_pwd
               })
               .SingleOrDefault();

                var src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";

                List<string> srclist = new List<string>();

                //Source connection establish
                using (MySqlConnection connection = new MySqlConnection(src_connstring))
                {
                    connection.Open();

                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = "SELECT COLUMN_NAME FROM information_schema.COLUMNS " +
                                          "WHERE TABLE_SCHEMA = '" + databasename + "' and TABLE_NAME = '" +
                                           sourcetable + "';";

                    //sourcetable + "' AND COLUMN_NAME NOT IN ('dataset_gid','scheduler_gid','delete_flag') ;";
                    int i = 1;

                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            srcfieldname.Add(new DatabaseInfo { Name = reader.GetString(0) });
                            var pplsrcfld = new PipelineSourcefield()
                            {
                                pplsourcefield_gid = 0,
                                pipeline_code = pipelinecode,
                                sourcefield_name = reader.GetString(0),
                                dataset_table_field = "",//"col" + i,
                                expressionfield_json = null,
                                sourcefieldmapping_flag = "N",
                                source_type = tvq_type,
                                created_by = user_code,
                                created_date = DateTime.Now,
                                delete_flag = "N"
                            };
                            dbContext.con_trn_tpplsourcefield.Add(pplsrcfld);
                            i++;
                        }
                    }
                    connection.Close();
                }

                // Save all changes to the database
                await dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                // Handle any exceptions here.
                Console.WriteLine("Error: " + ex.Message);
            }
            return srcfieldname;
        }

        [HttpPost]
        public bool ValidateFile_sheet([FromBody] validateexcelsheet obj)
        {
            var flag = false;
            var file_extension = "";
            try
            {
                file_extension = System.IO.Path.GetExtension(obj.file_Paths);

                if (file_extension == ".xls" || file_extension == ".xlsx")
                {
                    using (var fileStream = System.IO.File.Open(obj.file_Paths, FileMode.Open, FileAccess.Read))
                    {
                        IWorkbook workbook;
                        if (file_extension == ".xls")
                        {
                            workbook = new HSSFWorkbook(fileStream);
                        }
                        else // ".xlsx"
                        {
                            workbook = new XSSFWorkbook(fileStream);
                        }

                        ISheet sheet = workbook.GetSheet(obj.sheetName);
                        if (sheet != null && sheet.GetRow(0) != null)
                        {
                            flag = true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Log the exception
            }

            return flag;
        }

        [HttpPost]
        public bool ValidateFile_sheet_OLEDB([FromBody] validateexcelsheet obj)
        {
            var flag = false;
            var file_extension = "";
            try
            {
                file_extension = System.IO.Path.GetExtension(obj.file_Paths);

                if (file_extension == ".xls")
                {
                    using (FileStream fileStream = new FileStream(obj.file_Paths, FileMode.Open, FileAccess.Read))
                    {
                        HSSFWorkbook workbook = new HSSFWorkbook(fileStream);
                        HSSFSheet sheet = (HSSFSheet)workbook.GetSheet(obj.sheetName);
                        if (sheet != null && sheet.GetRow(0) != null)
                        {
                            flag = true;
                        }
                    }
                }
                else if (file_extension == ".xlsx")
                {
                    using (FileStream fileStream = new FileStream(obj.file_Paths, FileMode.Open, FileAccess.Read))
                    {
                        XSSFWorkbook workbook = new XSSFWorkbook(fileStream);
                        XSSFSheet sheet = (XSSFSheet)workbook.GetSheet(obj.sheetName);

                        if (sheet != null && sheet.GetRow(0) != null)
                        {
                            flag = true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return flag;

            }

            return flag;
        }

        [HttpGet]
        public IActionResult GetSourceFieldDropdown(string pipeline_code, string source_type)
        {
            try
            {
                var Lovsrcfield = dbContext.con_trn_tpplsourcefield
                .Where(p => p.pipeline_code == pipeline_code && (string.IsNullOrEmpty(source_type) || p.source_type == source_type) && p.delete_flag == "N")
                .Select(c => new SrcExpression
                {
                    //Id = c.pplsourcefield_gid,
                    //Name = c.sourcefield_name

                    Id = c.sourcefield_name,
                    Name = c.source_type == "Expression" ? "*" + c.sourcefield_name : c.sourcefield_name
                })
                .ToList();
                Lovsrcfield.Insert(0, new SrcExpression { Id = "-- Select --", Name = "-- Select --" });

                return Ok(Lovsrcfield);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        private bool IsSystemDatabase(string databaseName)
        {
            return databaseName.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("performance_schema", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("sys", StringComparison.OrdinalIgnoreCase);
        }
        [HttpGet]
        public async Task<IActionResult> GetTables(string connection_code, string databasename)
        {
            var connector = dbContext.con_mst_tconnection
                 .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                 .Select(p => new ConnectionModel
                 {
                     source_host_name = p.source_host_name,
                     source_port = p.source_port,
                     source_db_user = p.source_db_user,
                     source_db_pwd = p.source_db_pwd,
                     source_db_type = p.source_db_type
                 })
                 .SingleOrDefault();
            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }
                var connstring = "";
                List<TableAndView> tableviews = new List<TableAndView>();

                if (connector.source_db_type == "MySql")
                {
                    connstring = "server=" + connector.source_host_name + "; uid=" +
                             connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";


                    using (MySqlConnection connection = new MySqlConnection(connstring))
                    {
                        connection.Open();

                        MySqlCommand command = connection.CreateCommand();
                        command.CommandText = "SELECT TABLE_NAME FROM information_schema.TABLES " +
                                              "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + databasename + "';";
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string tableandview = reader.GetString(0);
                                if (!IsSystemDatabase(tableandview))
                                {
                                    tableviews.Add(new TableAndView { Name = tableandview });
                                }
                            }
                        }
                        connection.Close();
                    }
                }
                else if (connector.source_db_type == "Postgres")
                {
                    connstring = "Host=" + connector.source_host_name + "; Database=" + databasename + "; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    using (NpgsqlConnection connection = new NpgsqlConnection(connstring))
                    {
                        connection.Open();

                        NpgsqlCommand command = connection.CreateCommand();
                        command.CommandText = "SELECT (schemaname || '.' || tablename) as TABLE_NAME FROM pg_tables" +
                            " where schemaname not in ('information_schema','pg_catalog') " +
                            " and hasindexes = TRUE;";
                        using (NpgsqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string tableandview = reader.GetString(0);
                                if (!IsSystemDatabase(tableandview))
                                {
                                    tableviews.Add(new TableAndView { Name = tableandview });
                                }
                            }
                        }
                        connection.Close();
                    }
                }


                return Ok(tableviews);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetViews(string connection_code, string databasename)
        {
            var connector = dbContext.con_mst_tconnection
                 .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                 .Select(p => new ConnectionModel
                 {
                     source_host_name = p.source_host_name,
                     source_port = p.source_port,
                     source_db_user = p.source_db_user,
                     source_db_pwd = p.source_db_pwd,
                     source_db_type = p.source_db_type
                 })
                 .SingleOrDefault();
            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }
                var connstring = "";
                List<TableAndView> tableviews = new List<TableAndView>();

                if (connector.source_db_type == "MySql")
                {
                    connstring = "server=" + connector.source_host_name + "; uid=" +
                             connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";


                    using (MySqlConnection connection = new MySqlConnection(connstring))
                    {
                        connection.Open();

                        MySqlCommand command = connection.CreateCommand();
                        command.CommandText = "SELECT TABLE_NAME FROM information_schema.TABLES " +
                                              "WHERE TABLE_TYPE = 'View' AND TABLE_SCHEMA = '" + databasename + "';";
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string tableandview = reader.GetString(0);
                                if (!IsSystemDatabase(tableandview))
                                {
                                    tableviews.Add(new TableAndView { Name = tableandview });
                                }
                            }
                        }
                        connection.Close();
                    }
                }
                else if (connector.source_db_type == "Postgres")
                {
                    connstring = "Host=" + connector.source_host_name + "; Database=" + databasename + "; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    using (NpgsqlConnection connection = new NpgsqlConnection(connstring))
                    {
                        connection.Open();

                        NpgsqlCommand command = connection.CreateCommand();
                        command.CommandText = "SELECT (schemaname || '.' || viewname) as TABLE_NAME FROM pg_views" +
                            " where schemaname not in ('information_schema','pg_catalog') ";
                        using (NpgsqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string tableandview = reader.GetString(0);
                                if (!IsSystemDatabase(tableandview))
                                {
                                    tableviews.Add(new TableAndView { Name = tableandview });
                                }
                            }
                        }
                        connection.Close();
                    }
                }


                return Ok(tableviews);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetTargettable()
        {
            try
            {
                var Lovconn = dbContext.con_mst_tdataset
                .Select(c => new TargetTable
                {
                    dataset_code = c.dataset_code + "-" + c.table_name,
                    dataset_name = c.dataset_name
                })
                .ToList();

                return Ok(Lovconn);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetFieldNames(string connection_code, string databasename, string sourcetable, string targettable)
        {
            var connector = dbContext.con_mst_tconnection
                .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                .Select(p => new ConnectionModel
                {
                    source_host_name = p.source_host_name,
                    source_port = p.source_port,
                    source_db_user = p.source_db_user,
                    source_db_pwd = p.source_db_pwd
                })
                .SingleOrDefault();

            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }

                var src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";
                var trg_connstring = conn;

                List<string> srclist = new List<string>();
                List<string> trglist = new List<string>();

                //Source connection establish
                using (MySqlConnection connection = new MySqlConnection(src_connstring))
                {
                    connection.Open();

                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = "SELECT COLUMN_NAME FROM information_schema.COLUMNS " +
                                          "WHERE TABLE_SCHEMA = '" + databasename + "' and TABLE_NAME = '" +
                                           sourcetable + "';";

                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            srclist.Add(reader.GetString(0));
                        }
                    }
                    connection.Close();
                }

                //Target connection establish
                using (MySqlConnection connection = new MySqlConnection(trg_connstring))
                {
                    connection.Open();

                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = "SELECT COLUMN_NAME FROM information_schema.COLUMNS " +
                                          "WHERE TABLE_SCHEMA = " + trg_dbname + " and TABLE_NAME = '" +
                                           targettable + "';";

                    using (MySqlDataReader reader = command.ExecuteReader())
                    {

                        while (reader.Read())
                        {
                            trglist.Add(reader.GetString(0));
                        }
                    }
                    connection.Close();

                }
                var mergedList = (from t1 in srclist
                                  join t2 in trglist on srclist.IndexOf(t1) equals trglist.IndexOf(t2)
                                  select new TableFields { source_field_name = t1, target_field_name = t2 }
                               ).ToList();

                return Ok(mergedList);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetSourcetblFields(string connection_code, string databasename, string sourcetable)
        {
            var connector = dbContext.con_mst_tconnection
                .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                .Select(p => new ConnectionModel
                {
                    source_host_name = p.source_host_name,
                    source_port = p.source_port,
                    source_db_user = p.source_db_user,
                    source_db_pwd = p.source_db_pwd,
                    source_db_type = p.source_db_type,
                })
                .SingleOrDefault();

            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }

                var src_connstring = "";

                List<SourcetblFields> columnList = new List<SourcetblFields>();

                if (connector.source_db_type == "MySql")
                {
                    src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (MySqlConnection connection = new MySqlConnection(src_connstring))
                    {
                        connection.Open();

                        string query = "SELECT COLUMN_NAME AS src_field_name, COLUMN_NAME AS src_field_desc " +
                                       "FROM information_schema.COLUMNS " +
                                       "WHERE TABLE_SCHEMA = @DatabaseName AND TABLE_NAME = @TableName";

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", sourcetable);

                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("src_field_name"),
                                        Name = reader.GetString("src_field_desc")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                else if (connector.source_db_type == "Postgres")
                {
                    src_connstring = "Host=" + connector.source_host_name + "; Database=" + databasename + "; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (NpgsqlConnection connection = new NpgsqlConnection(src_connstring))
                    {
                        connection.Open();
                        string[] parts = sourcetable.Split('.');
                        databasename = parts[0];
                        sourcetable = parts[1];

                        string query = "SELECT COLUMN_NAME AS src_field_name, COLUMN_NAME AS src_field_desc " +
                                       "FROM information_schema.COLUMNS " +
                                       "WHERE TABLE_SCHEMA = @DatabaseName AND TABLE_NAME = @TableName";

                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", sourcetable);

                            using (NpgsqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("src_field_name"),
                                        Name = reader.GetString("src_field_desc")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                return Ok(columnList);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetKeyFields(string connection_code, string databasename, string tablename)
        {
            var connector = dbContext.con_mst_tconnection
                  .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                  .Select(p => new ConnectionModel
                  {
                      source_host_name = p.source_host_name,
                      source_port = p.source_port,
                      source_db_user = p.source_db_user,
                      source_db_pwd = p.source_db_pwd,
                      source_db_type = p.source_db_type,
                  })
                  .SingleOrDefault();

            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }

                var src_connstring = "";

                List<SourcetblFields> columnList = new List<SourcetblFields>();

                if (connector.source_db_type == "MySql")
                {
                    databasename = trg_dbname;
                    src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (MySqlConnection connection = new MySqlConnection(src_connstring))
                    {
                        connection.Open();

                        string query = "SELECT t.constraint_type as constraint_type,GROUP_CONCAT(k.column_name " +
                                       "ORDER BY k.ordinal_position ASC SEPARATOR ', ') AS column_names " +
                                       "FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k " +
                                       "USING (constraint_name, table_schema, table_name) WHERE t.constraint_type IN ('PRIMARY KEY', 'UNIQUE') " +
                                       "AND t.table_schema = @DatabaseName AND t.table_name = @TableName GROUP BY t.constraint_type;";

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", tablename);

                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("constraint_type"),
                                        Name = reader.GetString("column_names")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                else if (connector.source_db_type == "Postgres")
                {
                    src_connstring = "Host=" + connector.source_host_name + "; Database=" + databasename + "; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (NpgsqlConnection connection = new NpgsqlConnection(src_connstring))
                    {
                        connection.Open();
                        string[] parts = tablename.Split('.');
                        tablename = parts[1];

                        string query = "SELECT t.constraint_type AS constraint_type," +
                                        "STRING_AGG(k.column_name, ', ' ORDER BY k.ordinal_position) AS column_names " +
                                        "FROM information_schema.table_constraints t " +
                                        "JOIN information_schema.key_column_usage k ON " +
                                        "t.constraint_name = k.constraint_name AND t.table_schema = k.table_schema " +
                                        "AND t.table_name = k.table_name " +
                                        "WHERE t.constraint_type IN('PRIMARY KEY', 'UNIQUE') " +
                                        "AND t.table_catalog = @DatabaseName AND t.table_name = @TableName " +
                                        "GROUP BY t.constraint_type;";

                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", tablename);

                            using (NpgsqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("constraint_type"),
                                        Name = reader.GetString("column_names")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                return Ok(columnList);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetTargetKeyFields(string tablename)
        {
            try
            {
                List<TargettblKeyfields> columnList = new List<TargettblKeyfields>();

                if (dbtype == "Mysql")
                {
                    //target connection establish
                    using (MySqlConnection connection = new MySqlConnection(targetconnectionString))
                    {
                        connection.Open();

                        string query = "SELECT distinct a.dataset_field_name AS keyfield, a.dataset_field_desc AS keyfield_desc " +
                                       "FROM con_mst_tdataset_field as a " +
                                       "inner join con_trn_tpplfieldmapping as b " +
                                       "on a.dataset_code = b.dataset_code and b.ppl_field_name != '-- Select --' and b.ppl_field_name != '' " +
                                       "and a.dataset_field_name = b.dataset_field_name and b.delete_flag = 'N' " +
                                       "WHERE a.dataset_code = @tablename  and a.delete_flag = 'N' ";

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@TableName", tablename);

                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    TargettblKeyfields column = new TargettblKeyfields
                                    {
                                        ID = reader.GetString("keyfield"),
                                        Name = reader.GetString("keyfield_desc")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }

                return Ok(columnList);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetTargettblFields_old(string dataset_code)
        {

            var ds_code = await dbContext.con_mst_tdataset_field
                        .Where(a => a.dataset_code == dataset_code && a.delete_flag == "N")
                        .Select(a => new
                        {
                            dataset_field_desc = a.dataset_field_desc,
                            dataset_field_name = a.dataset_field_name
                        })
                        .ToListAsync();
            try
            {
                if (ds_code == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(ds_code);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> AddpplFieldMapping([FromBody] AddpplFieldMappingRequest addpplfieldmap)
        {
            var ds_codes = await dbContext.con_mst_tdataset_field
                .Where(a => a.dataset_code == addpplfieldmap.dataset_code && a.delete_flag == "N" && a.active_status == "Y")
                .Select(a => new
                {
                    dataset_field_desc = a.dataset_field_desc,
                    dataset_field_name = a.dataset_field_name,
                    field_mandatory = a.field_mandatory
                })
                .ToListAsync();

            if (ds_codes.Count == 0 && addpplfieldmap.dataset_code != "")
            {
                // Delete the previous Fieldmapping record against pipelinecode
                var pplfieldMappingToDelete = await dbContext.con_trn_tpplfieldmapping
                .Where(p => p.pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == "N")
                .ToListAsync();

                if (pplfieldMappingToDelete.Any())
                {
                    dbContext.con_trn_tpplfieldmapping.RemoveRange(pplfieldMappingToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Delete the previous Conditions record against pipelinecode
                var ConditionToDelete = await dbContext.con_trn_tpplcondition
                .Where(p => p.pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == "N")
                .ToListAsync();

                if (ConditionToDelete.Any())
                {
                    dbContext.con_trn_tpplcondition.RemoveRange(ConditionToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Delete the previous datasetprocessingheader record against pipelinecode
                var DataprocessingHeaderToDelete = await dbContext.con_mst_tdataprocessingheader
                .Where(p => p.dataprocessingheader_pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == 'N')
                .ToListAsync();

                if (DataprocessingHeaderToDelete.Any())
                {
                    dbContext.con_mst_tdataprocessingheader.RemoveRange(DataprocessingHeaderToDelete);
                    await dbContext.SaveChangesAsync();
                }

                return Ok("Target dataset Fields are not available..!");
            }

            var count = dbContext.con_trn_tpplfieldmapping.Count();
            var maxId = count > 0 ? dbContext.con_trn_tpplfieldmapping.Max(entity => entity.pplfieldmapping_gid) + 1 : 1;

            try
            {
                // Delete the previous Fieldmapping record against pipelinecode
                var pplfieldMappingToDelete = await dbContext.con_trn_tpplfieldmapping
                .Where(p => p.pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == "N")
                .ToListAsync();

                if (pplfieldMappingToDelete.Any())
                {
                    dbContext.con_trn_tpplfieldmapping.RemoveRange(pplfieldMappingToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Delete the previous Conditions record against pipelinecode
                var ConditionToDelete = await dbContext.con_trn_tpplcondition
                .Where(p => p.pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == "N")
                .ToListAsync();

                if (ConditionToDelete.Any())
                {
                    dbContext.con_trn_tpplcondition.RemoveRange(ConditionToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Delete the previous datasetprocessingheader record against pipelinecode
                var DataprocessingHeaderToDelete = await dbContext.con_mst_tdataprocessingheader
                .Where(p => p.dataprocessingheader_pipeline_code == addpplfieldmap.pipeline_code && p.delete_flag == 'N')
                .ToListAsync();

                if (DataprocessingHeaderToDelete.Any())
                {
                    dbContext.con_mst_tdataprocessingheader.RemoveRange(DataprocessingHeaderToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Insert in fieldmapping table
                foreach (var ds_code in ds_codes)
                {
                    var pplmap = new PipelineMapping()
                    {
                        pplfieldmapping_gid = maxId, // Assign the same maxId for all records or generate a unique ID as needed
                        pipeline_code = addpplfieldmap.pipeline_code,
                        dataset_code = addpplfieldmap.dataset_code,
                        ppl_field_name = "-- Select --",
                        pplfieldmapping_flag = (ds_code.field_mandatory == "Y") ? 1 : 0,
                        dataset_field_name = ds_code.dataset_field_name,
                        created_by = addpplfieldmap.created_by,
                        created_date = DateTime.Now,
                        delete_flag = "N"
                    };

                    await dbContext.con_trn_tpplfieldmapping.AddAsync(pplmap);
                    maxId++; // Increment maxId for the next record
                }

                await dbContext.SaveChangesAsync();
                return Ok("Inserted Successfully");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> UpdatepplFieldMapping([FromBody] List<UpdatepplFieldMappingRequest> updpplfieldmap)
        {

            var msg = "";
            var pplcodefldmapp = await dbContext.con_trn_tpplfieldmapping
               .Where(p => p.pipeline_code == updpplfieldmap[0].pipeline_code && p.delete_flag == "N")
               .ToListAsync();

            //var pplsrcfldmapp = await dbContext.con_trn_tpplsourcefield
            //  .Where(p => p.pipeline_code == updpplfieldmap[0].pipeline_code && p.delete_flag == "N")
            //  .ToListAsync();

            //int y = 1;

            try
            {
                for (int j = 0; pplcodefldmapp.Count > j; j++)
                {
                    if (pplcodefldmapp.Any())
                    {
                        pplcodefldmapp[j].pplfieldmapping_flag = 0;
                        pplcodefldmapp[j].default_value = "";
                        pplcodefldmapp[j].ppl_field_name = "";
                        pplcodefldmapp[j].updated_date = DateTime.Now;
                        pplcodefldmapp[j].updated_by = updpplfieldmap[j].updated_by;

                        // Save the changes to the database
                        await dbContext.SaveChangesAsync();
                    }
                }

                //for (int m = 0; pplsrcfldmapp.Count > m; m++)
                //{
                //    if (pplsrcfldmapp.Any())
                //    {
                //        pplsrcfldmapp[m].sourcefieldmapping_flag = "N";
                //        pplsrcfldmapp[m].cast_dataset_table_field = "";
                //        pplsrcfldmapp[m].dataset_table_field = "";
                //        pplsrcfldmapp[m].updated_date = DateTime.Now;
                //        pplsrcfldmapp[m].updated_by = "Admin";

                //        // Save the changes to the database
                //        await dbContext.SaveChangesAsync();
                //    }
                //}


                var srcmap_count = dbContext.con_trn_tpplsourcefield
                   .Where(x =>
                       x.sourcefieldmapping_flag == "Y" &&
                       x.delete_flag == "N" &&
                       x.pipeline_code == updpplfieldmap[0].pipeline_code)
                   .Count();
                int colval = srcmap_count + 1;


                for (int i = 0; updpplfieldmap.Count > i; i++)
                {
                    var pplfldmappToUpdate = await dbContext.con_trn_tpplfieldmapping
                        .Where(p => p.pplfieldmapping_gid == Convert.ToInt32(updpplfieldmap[i].pplfieldmapping_gid) && p.delete_flag == "N")
                        .FirstOrDefaultAsync();

                    if (pplfldmappToUpdate != null)
                    {
                        pplfldmappToUpdate.ppl_field_name = updpplfieldmap[i].ppl_field_name;
                        pplfldmappToUpdate.default_value = updpplfieldmap[i].default_value;
                        pplfldmappToUpdate.pplfieldmapping_flag = updpplfieldmap[i].pplfieldmapping_flag;
                        pplfldmappToUpdate.updated_date = DateTime.Now;
                        pplfldmappToUpdate.updated_by = updpplfieldmap[i].updated_by;

                        // Save the changes to the database
                        await dbContext.SaveChangesAsync();
                        msg = "Updated Successfully..!";
                    }

                    var dsfield = dbContext.con_mst_tdataset_field
                      .Where(p => p.dataset_field_name == updpplfieldmap[i].dataset_field_name
                      && p.dataset_code == updpplfieldmap[i].dataset_code)
                      .Select(p => new DataSetField
                      {
                          dataset_field_name = p.dataset_field_name,
                          dataset_table_field = p.dataset_table_field,
                          dataset_field_type = p.dataset_field_type,
                          dataset_code = p.dataset_code
                      })
                      .SingleOrDefault();

                    var pplsrcfield = await dbContext.con_trn_tpplsourcefield
                         .Join(
                             dbContext.con_trn_tpplfieldmapping,
                             p => p.pipeline_code,
                             a => a.pipeline_code,
                             (p, a) => new { SourceField = p, AnotherField = a }
                         )
                         .Where(joinedData => joinedData.AnotherField.dataset_code == dsfield.dataset_code
                             && joinedData.SourceField.delete_flag == "N"
                             && joinedData.SourceField.pipeline_code == updpplfieldmap[i].pipeline_code
                             && joinedData.SourceField.sourcefield_name == updpplfieldmap[i].ppl_field_name
                         )
                         .FirstOrDefaultAsync();

                    if (updpplfieldmap[i].ppl_field_name != "-- Select --")
                    {

                        var pplsrcfldgid = await dbContext.con_trn_tpplsourcefield
                        .Where(p => p.pipeline_code == pplsrcfield.SourceField.pipeline_code
                                && p.sourcefield_name == updpplfieldmap[i].ppl_field_name
                                && p.sourcefieldmapping_flag == "N"
                                && p.delete_flag == "N")
                        .Select(a => new
                        {
                            pplsourcefield_gid = a.pplsourcefield_gid,
                            dataset_table_field = a.pplsourcefield_gid > 0 ? "col" + colval : ""//a.dataset_table_field
                        })
                        .ToListAsync();


                        if (pplsrcfldgid != null && pplsrcfldgid.Count > 0)
                        {
                            var pplsrcfldUpdate = await dbContext.con_trn_tpplsourcefield.FindAsync(Convert.ToInt32(pplsrcfldgid[0].pplsourcefield_gid));

                            if (pplsrcfldUpdate != null)
                            {

                                var v_cast_dataset_table_field = "";
                                pplsrcfldUpdate.dataset_table_field = "col" + colval;
                                pplsrcfldUpdate.dataset_table_field_sno = colval;
                                pplsrcfldUpdate.sourcefieldmapping_flag = "Y";
                                if (dbtype == "Mysql")
                                {

                                    if (dsfield.dataset_field_type == "TEXT")
                                    {
                                        v_cast_dataset_table_field = pplsrcfldgid[0].dataset_table_field;
                                    }
                                    else if (dsfield.dataset_field_type == "DATE")
                                    {
                                        v_cast_dataset_table_field = "STR_TO_DATE(if(" + pplsrcfldgid[0].dataset_table_field + "='',null," + pplsrcfldgid[0].dataset_table_field + "),'#DATE_FORMAT#')";
                                    }
                                    else if (dsfield.dataset_field_type == "NUMERIC")
                                    {
                                        //v_cast_dataset_table_field = "CAST(" + pplsrcfldgid[0].dataset_table_field + " AS DECIMAL(15,2))";
                                        v_cast_dataset_table_field = "CAST(if(" + pplsrcfldgid[0].dataset_table_field + "='',0," + pplsrcfldgid[0].dataset_table_field + ") AS DECIMAL(15,2))";

                                    }
                                    else if (dsfield.dataset_field_type == "INTEGER")
                                    {
                                        v_cast_dataset_table_field = "CAST(if(" + pplsrcfldgid[0].dataset_table_field + "='' or " + pplsrcfldgid[0].dataset_table_field + "= null " + ",0," + pplsrcfldgid[0].dataset_table_field + ") AS SIGNED)";
                                    }
                                    else if (dsfield.dataset_field_type == "DATETIME")
                                    {
                                        v_cast_dataset_table_field = "STR_TO_DATE(if(" + pplsrcfldgid[0].dataset_table_field + "='',null," + pplsrcfldgid[0].dataset_table_field + "),'#DATETIME_FORMAT#')";
                                        //v_cast_dataset_table_field = "STR_TO_DATE(" + pplsrcfldgid[0].dataset_table_field + ",'#DATETIME_FORMAT#')";
                                    }
                                    pplsrcfldUpdate.cast_dataset_table_field = v_cast_dataset_table_field;
                                    pplsrcfldUpdate.updated_by = updpplfieldmap[0].updated_by;
                                    pplsrcfldUpdate.updated_date = DateTime.Now;
                                }

                                //y = y + 1;
                                colval = colval + 1;

                                await dbContext.SaveChangesAsync();

                                msg = "Record Updated Successfully";
                            }
                            pplsrcfldUpdate = null;
                        }

                    }

                }

                return Ok(msg);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }

        }

        [HttpPost]
        public IActionResult SourcetoTargetPush([FromBody] SourceToTargetPushdata srctotrgpushdata)
        {
            var connector = dbContext.con_mst_tconnection
                .Where(p => p.connection_code == srctotrgpushdata.connection_code && p.delete_flag == "N")
                .Select(p => new ConnectionModel
                {
                    source_host_name = p.source_host_name,
                    source_port = p.source_port,
                    source_db_user = p.source_db_user,
                    source_db_pwd = p.source_db_pwd,
                    source_db_type = p.source_db_type,
                })
                .SingleOrDefault();

            try
            {
                var count = 0;
                if (connector == null)
                {
                    return NotFound("No Data found");
                }

                DataTable dt = new DataTable();

                if (connector.source_db_type == "MySql")
                {
                    var conn = GetMySqlServerConnection(connector.source_host_name,
                                                        srctotrgpushdata.databasename,
                                                        connector.source_db_user,
                                                        connector.source_db_pwd
                                                        );
                    dt = GetDataTableFromMySQLServer(conn,
                                                     srctotrgpushdata.sourcetable,
                                                     srctotrgpushdata.source_field_columns,
                                                     srctotrgpushdata.defaultvalue,
                                                     srctotrgpushdata.updated_time_stamp,
                                                     srctotrgpushdata.pull_days);
                    count = dt.Rows.Count;
                    PushDataToMySQL(connector.source_db_type, dt, srctotrgpushdata.targettable, srctotrgpushdata.upload_mode, srctotrgpushdata.primary_key);
                }
                else if (connector.source_db_type == "Postgres")
                {
                    var conn = GetPostgresServerConnection(connector.source_host_name,
                                                           srctotrgpushdata.databasename,
                                                           connector.source_db_user,
                                                           connector.source_db_pwd
                                                           );
                    dt = GetDataTableFromPostgreSQLServer(conn, srctotrgpushdata.sourcetable, srctotrgpushdata.source_field_columns, srctotrgpushdata.defaultvalue);
                    count = dt.Rows.Count;
                    PushDataToMySQL(connector.source_db_type, dt, srctotrgpushdata.targettable, srctotrgpushdata.upload_mode, srctotrgpushdata.primary_key);
                }

                return Ok("Totally " + count + " Data Transfered Successfully..!");

            }
            catch (Exception ex)
            {
                return Ok($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> AddPipeline([FromBody] AddPipelineRequest addPipelineRequest)
        {
            try
            {
                //string[] parts = addPipelineRequest.table_view_query_desc.Split('.');
                //addPipelineRequest.table_view_query_desc = parts[1];

                int count = dbContext.con_mst_tpipeline.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_mst_tpipeline.Max(entity => entity.pipeline_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }

                var ppl = new Pipeline()
                {
                    pipeline_gid = 0,//Guid.NewGuid(),
                    pipeline_code = "PIPE_" + ((maxId.ToString()).PadLeft(4, '0')),
                    pipeline_name = addPipelineRequest.pipeline_name,
                    pipeline_desc = addPipelineRequest.pipeline_desc,
                    connection_code = addPipelineRequest.connection_code,
                    db_name = addPipelineRequest.db_name,
                    table_view_query_type = addPipelineRequest.table_view_query_type,
                    table_view_query_desc = addPipelineRequest.table_view_query_desc,
                    //custom_query = addPipelineRequest.custom_query,
                    target_dataset_code = addPipelineRequest.target_dataset_code,
                    pipeline_status = addPipelineRequest.pipeline_status,
                    created_date = addPipelineRequest.created_date,
                    created_by = addPipelineRequest.created_by,
                    updated_date = addPipelineRequest.updated_date,
                    updated_by = addPipelineRequest.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_mst_tpipeline.AddAsync(ppl);
                await dbContext.SaveChangesAsync();

                var lastInsertedId = ppl.pipeline_gid;

                return Ok(lastInsertedId);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> UpdatePipeline([FromBody] UpdatePipelineRequest updatePipelineRequest)
        {
            var pipeline = await dbContext.con_mst_tpipeline.FindAsync(Convert.ToInt32(updatePipelineRequest.pipeline_gid));
            try
            {
                //string[] parts = updatePipelineRequest.table_view_query_desc.Split('.');
                //updatePipelineRequest.table_view_query_desc = parts[1];

                if (pipeline != null)
                {
                    pipeline.pipeline_name = updatePipelineRequest.pipeline_name;
                    pipeline.pipeline_desc = updatePipelineRequest.pipeline_desc;
                    pipeline.connection_code = updatePipelineRequest.connection_code;
                    pipeline.db_name = updatePipelineRequest.db_name;
                    pipeline.source_file_name = updatePipelineRequest.source_file_name;
                    pipeline.sheet_name = updatePipelineRequest.sheet_name;
                    pipeline.table_view_query_type = updatePipelineRequest.table_view_query_type;
                    pipeline.table_view_query_desc = updatePipelineRequest.table_view_query_desc;
                    pipeline.target_dataset_code = updatePipelineRequest.target_dataset_code;
                    pipeline.updated_date = updatePipelineRequest.updated_date;
                    pipeline.updated_by = updatePipelineRequest.updated_by;
                    pipeline.pipeline_status = updatePipelineRequest.pipeline_status;


                    await dbContext.SaveChangesAsync();

                    return Ok("Record Updated Successfully");

                }
                return NotFound("Not Found TO Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> DeletePipeline([FromBody] string pipeline_code)
        {
            try
            {
                var pipelineToDelete = dbContext.con_mst_tpipeline.FirstOrDefault(p => p.pipeline_code == pipeline_code);
                var pipelineFldmapToDelete = await dbContext.con_trn_tpplfieldmapping
                    .Where(p => p.pipeline_code == pipeline_code)
                    .ToListAsync();
                var pipelineFinzToDelete = dbContext.con_trn_tpplfinalization.FirstOrDefault(p => p.pipeline_code == pipeline_code);

                if (pipelineToDelete != null)
                {
                    pipelineToDelete.pipeline_status = "Inactive";
                    //pipelineToDelete.delete_flag = "Y";
                    await dbContext.SaveChangesAsync();
                    // dbContext.Remove(pipelineToDelete);
                }

                //if (pipelineFldmapToDelete != null && pipelineFldmapToDelete.Count > 0)
                //{
                //    dbContext.con_trn_tpplfieldmapping.RemoveRange(pipelineFldmapToDelete);
                //    await dbContext.SaveChangesAsync();
                //}

                //if (pipelineFinzToDelete != null)
                //{
                //    pipelineFinzToDelete.delete_flag = "Y";
                //    await dbContext.SaveChangesAsync();
                //}


                return Ok("Deleted Successfully..!");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }
        #endregion

        #region Pipelinesourcefield
        [HttpPost]
        public async Task<IActionResult> Addpplsourcefield([FromBody] AddpplSourceFieldRequest addpplsourcefld)
        {
            try
            {

                int count = dbContext.con_trn_tpplsourcefield.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_trn_tpplsourcefield.Max(entity => entity.pplsourcefield_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }

                var dplchk = await dbContext.con_trn_tpplsourcefield
                   .Where(p => p.pipeline_code == addpplsourcefld.pipeline_code && p.sourcefield_name.Trim() == addpplsourcefld.sourcefield_name.Trim()
                   && p.delete_flag == "N")
                   .ToListAsync();

                if (dplchk.Count > 0)
                {
                    return Ok("Duplicate Record..!");
                }
                else
                {
                    // Extract values inside square brackets
                    string[] matches = Regex.Matches(addpplsourcefld.sourcefield_expression, @"\[([^\]]+)\]")
                                       .Cast<Match>()
                                       .Select(m => m.Groups[1].Value)
                                       .ToArray();

                    // Create a list of anonymous objects with the extracted values
                    var jsonObjects = matches.Select(value => new { source_field = value }).ToList();

                    var srcmap_count = dbContext.con_trn_tpplsourcefield
                       .Where(x =>
                           x.sourcefieldmapping_flag == "Y" &&
                           x.delete_flag == "N" &&
                           x.pipeline_code == addpplsourcefld.pipeline_code)
                       .Count();

                    int colval = srcmap_count + 1;

                    for (int n = 0; n < matches.Count(); n++)
                    {
                        var src_field = matches[n];

                        var recordsToUpdate = dbContext.con_trn_tpplsourcefield
                        .Where(s => s.sourcefield_name == src_field &&
                                    s.pipeline_code == addpplsourcefld.pipeline_code &&
                                    (s.dataset_table_field == "" || s.dataset_table_field == null) &&
                                    s.delete_flag == "N")
                        .ToList();

                        foreach (var recordToUpdate in recordsToUpdate)
                        {
                            var v_cast_dataset_table_field = "";
                            var v_dataset_table_field = "col" + colval;
                            if (!string.IsNullOrEmpty(recordToUpdate.sourcefield_name))
                            {

                                if (addpplsourcefld.sourcefield_datatype == "TEXT")
                                {
                                    v_cast_dataset_table_field = v_dataset_table_field;
                                }
                                else if (addpplsourcefld.sourcefield_datatype == "DATE")
                                {
                                    v_cast_dataset_table_field = "STR_TO_DATE(if(" + v_dataset_table_field + "='',null," + v_dataset_table_field + "),'#DATE_FORMAT#')";
                                }
                                else if (addpplsourcefld.sourcefield_datatype == "NUMERIC")
                                {
                                    v_cast_dataset_table_field = "CAST(if(" + v_dataset_table_field + "='',0," + v_dataset_table_field + ") AS DECIMAL(15,2))";
                                }
                                else if (addpplsourcefld.sourcefield_datatype == "INTEGER")
                                {
                                    v_cast_dataset_table_field = "CAST(if(" + v_dataset_table_field + "='' or " + v_dataset_table_field + "= null " + ",0," + v_dataset_table_field + ") AS SIGNED)";
                                }
                                else if (addpplsourcefld.sourcefield_datatype == "DATETIME")
                                {
                                    v_cast_dataset_table_field = "STR_TO_DATE(if(" + v_dataset_table_field + "='',null," + v_dataset_table_field + "),'#DATETIME_FORMAT#')";
                                }

                                recordToUpdate.sourcefieldmapping_flag = "Y";
                                recordToUpdate.dataset_table_field = "col" + colval;
                                recordToUpdate.cast_dataset_table_field = v_cast_dataset_table_field;
                                recordToUpdate.dataset_table_field_sno = colval;
                                recordToUpdate.updated_by = addpplsourcefld.updated_by;
                                recordToUpdate.updated_date = DateTime.Now;
                                await dbContext.SaveChangesAsync();
                                colval = colval + 1;
                            }
                        }

                    }

                    List<string> matchList = matches.ToList();
                    // Serialize the list to a JSON string
                    string datasetTableFieldJson = JsonConvert.SerializeObject(jsonObjects);


                    //Insert in  sourcefield table 
                    var pplsrcfld = new PipelineSourcefield()
                    {
                        pplsourcefield_gid = 0,//Guid.NewGuid(),
                        pipeline_code = addpplsourcefld.pipeline_code,
                        sourcefield_name = addpplsourcefld.sourcefield_name.Trim(),
                        sourcefield_datatype = addpplsourcefld.sourcefield_datatype,
                        sourcefield_expression = addpplsourcefld.sourcefield_expression,
                        source_type = addpplsourcefld.source_type,
                        dataset_table_field = "col" + colval,
                        dataset_table_field_sno = colval,
                        expressionfield_json = datasetTableFieldJson,
                        sourcefieldmapping_flag = "N",
                        created_date = addpplsourcefld.created_date,
                        created_by = addpplsourcefld.created_by,
                        updated_date = addpplsourcefld.updated_date,
                        updated_by = addpplsourcefld.updated_by,
                        delete_flag = "N"
                    };
                    await dbContext.con_trn_tpplsourcefield.AddAsync(pplsrcfld);
                    await dbContext.SaveChangesAsync();

                    return Ok("Record Inserted Successfully");
                }

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> Updatepplsourcefield([FromBody] UpdatepplSourceFieldRequest updatepplsourcefld)
        {
            var pplsrcfield = await dbContext.con_trn_tpplsourcefield.FindAsync(Convert.ToInt32(updatepplsourcefld.pplsourcefield_gid));
            try
            {

                if (pplsrcfield != null)
                {
                    var dplchkchanges = await dbContext.con_trn_tpplsourcefield
                    .Where(p => p.pplsourcefield_gid == Convert.ToInt32(updatepplsourcefld.pplsourcefield_gid)
                     && p.sourcefield_name.Trim() != updatepplsourcefld.sourcefield_name.Trim()
                     && p.delete_flag == "N")
                    .ToListAsync();

                    if (dplchkchanges.Count > 0)
                    {
                        var dplchk = await dbContext.con_trn_tpplsourcefield
                        .Where(p => p.pipeline_code == updatepplsourcefld.pipeline_code
                         && p.sourcefield_name.Trim() == updatepplsourcefld.sourcefield_name.Trim()
                         && p.delete_flag == "N")
                        .ToListAsync();

                        if (dplchk.Count >= 1)
                        {
                            return Ok("Duplicate Record..!");
                        }
                    }


                    // Extract values inside square brackets
                    var matches = Regex.Matches(updatepplsourcefld.sourcefield_expression, @"\[([^\]]+)\]")
                                       .Cast<Match>()
                                       .Select(m => m.Groups[1].Value)
                                       .ToArray();

                    // Create a list of anonymous objects with the extracted values
                    var jsonObjects = matches.Select(value => new { source_field = value }).ToList();

                    List<string> matchList = matches.ToList();
                    // Serialize the list to a JSON string
                    string datasetTableFieldJson = JsonConvert.SerializeObject(jsonObjects);

                    var srcmap_count = dbContext.con_trn_tpplsourcefield
                       .Where(x =>
                           x.sourcefieldmapping_flag == "Y" &&
                           x.delete_flag == "N" &&
                           x.pipeline_code == updatepplsourcefld.pipeline_code)
                       .Count();

                    int colval = srcmap_count + 1;

                    var recordsToUpdate = dbContext.con_trn_tpplsourcefield
                    .Where(s => matchList.Contains(s.sourcefield_name) &&
                                s.pipeline_code == updatepplsourcefld.pipeline_code &&
                                //s.sourcefield_name == "" &&
                                s.delete_flag == "N")
                    .ToList();


                    foreach (var recordToUpdate in recordsToUpdate)
                    {
                        var v_cast_dataset_table_field = "";
                        var v_dataset_table_field = "";
                        int v_dataset_table_field_sno = 0;

                        if (recordToUpdate.sourcefieldmapping_flag == "Y")
                        {
                            v_dataset_table_field = recordToUpdate.dataset_table_field;
                            v_dataset_table_field_sno = recordToUpdate.dataset_table_field_sno;
                        }
                        else
                        {
                            v_dataset_table_field = "col" + colval;
                            v_dataset_table_field_sno = colval;
                            colval = colval + 1;
                        }

                        if (!string.IsNullOrEmpty(recordToUpdate.sourcefield_name))
                        {

                            if (updatepplsourcefld.sourcefield_datatype == "TEXT")
                            {
                                v_cast_dataset_table_field = v_dataset_table_field;
                            }
                            else if (updatepplsourcefld.sourcefield_datatype == "DATE")
                            {
                                v_cast_dataset_table_field = "STR_TO_DATE(if(" + v_dataset_table_field + "='',null," + v_dataset_table_field + "),'#DATE_FORMAT#')";
                            }
                            else if (updatepplsourcefld.sourcefield_datatype == "NUMERIC")
                            {
                                v_cast_dataset_table_field = "CAST(if(" + v_dataset_table_field + "='',0," + v_dataset_table_field + ") AS DECIMAL(15,2))";

                            }
                            else if (updatepplsourcefld.sourcefield_datatype == "INTEGER")
                            {
                                v_cast_dataset_table_field = "CAST(if(" + v_dataset_table_field + "='' or " + v_dataset_table_field + "= null " + ",0," + v_dataset_table_field + ") AS SIGNED)";
                            }
                            else if (updatepplsourcefld.sourcefield_datatype == "DATETIME")
                            {
                                v_cast_dataset_table_field = "STR_TO_DATE(if(" + v_dataset_table_field + "='',null," + v_dataset_table_field + "),'#DATETIME_FORMAT#')";
                            }

                            recordToUpdate.sourcefieldmapping_flag = "Y";
                            recordToUpdate.dataset_table_field = v_dataset_table_field;
                            recordToUpdate.dataset_table_field_sno = v_dataset_table_field_sno;
                            recordToUpdate.cast_dataset_table_field = v_cast_dataset_table_field;
                            //recordToUpdate.sourcefield_sno = 
                        }
                    }

                    //dbContext.SaveChanges(); // Commit the updates to the database

                    pplsrcfield.sourcefield_name = updatepplsourcefld.sourcefield_name.Trim();
                    pplsrcfield.sourcefield_datatype = updatepplsourcefld.sourcefield_datatype;
                    pplsrcfield.sourcefield_expression = updatepplsourcefld.sourcefield_expression;
                    pplsrcfield.expressionfield_json = datasetTableFieldJson;
                    pplsrcfield.updated_date = updatepplsourcefld.updated_date;
                    pplsrcfield.updated_by = updatepplsourcefld.updated_by;

                    await dbContext.SaveChangesAsync();

                    return Ok("Record Updated Successfully");
                }
                return NotFound("Not Found TO Update");

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> Deletepplsourcefield(int pplsourcefield_gid)
        {
            try
            {
                var pplsrcfieldToDelete = dbContext.con_trn_tpplsourcefield.FirstOrDefault(p => p.pplsourcefield_gid == pplsourcefield_gid);

                if (pplsrcfieldToDelete != null)
                {
                    var pplfieldmappToDelete = dbContext.con_trn_tpplfieldmapping.FirstOrDefault(p => p.ppl_field_name == pplsrcfieldToDelete.sourcefield_name
                    && p.pipeline_code == pplsrcfieldToDelete.pipeline_code && p.delete_flag == "N");
                    if (pplfieldmappToDelete != null)
                    {
                        pplfieldmappToDelete.ppl_field_name = "-- Select --";
                        await dbContext.SaveChangesAsync();
                    }

                    pplsrcfieldToDelete.delete_flag = "Y";
                    await dbContext.SaveChangesAsync();
                }
                return Ok("Deleted Successfully..!");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> DeletepplCondition(int pplcondition_gid)
        {
            try
            {
                var pplCondToDelete = dbContext.con_trn_tpplcondition.FirstOrDefault(p => p.pplcondition_gid == pplcondition_gid);

                if (pplCondToDelete != null)
                {
                    pplCondToDelete.delete_flag = "Y";
                    await dbContext.SaveChangesAsync();
                }
                return Ok("Deleted Successfully..!");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        #endregion

        #region Pipeline Field Mapping
        [HttpPost]
        public async Task<IActionResult> AddpplFieldMapping_old([FromBody] AddpplFieldMappingRequest addpplfieldmap)
        {
            try
            {

                int count = dbContext.con_trn_tpplfieldmapping.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_trn_tpplfieldmapping.Max(entity => entity.pplfieldmapping_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }

                var pplmap = new PipelineMapping()
                {
                    pplfieldmapping_gid = 0,//Guid.NewGuid(),
                    pipeline_code = addpplfieldmap.pipeline_code,
                    ppl_field_name = addpplfieldmap.ppl_field_name,
                    dataset_field_name = addpplfieldmap.dataset_field_name,
                    default_value = addpplfieldmap.default_value,
                    created_date = addpplfieldmap.created_date,
                    created_by = addpplfieldmap.created_by,
                    updated_date = addpplfieldmap.updated_date,
                    updated_by = addpplfieldmap.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_trn_tpplfieldmapping.AddAsync(pplmap);
                await dbContext.SaveChangesAsync();

                return Ok("Record Inserted Successfully");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }
        [HttpGet]
        public async Task<IActionResult> GetpplFieldMapping(string pipelinecode)
        {
            var ds1 = dbContext.con_mst_tpipeline
               .Where(p => p.pipeline_code == pipelinecode && p.delete_flag == "N")
               .Select(p => new Pipeline
               {
                   target_dataset_code = p.target_dataset_code,
               })
               .SingleOrDefault();

            var ds_code = await dbContext.con_trn_tpplfieldmapping
                .Where(a => a.pipeline_code == pipelinecode && a.delete_flag == "N")
                .Select(a => new
                {
                    pplfieldmapping_gid = a.pplfieldmapping_gid,
                    pipeline_code = a.pipeline_code,
                    pplfieldmapping_flag = a.pplfieldmapping_flag,
                    ppl_field_name = a.ppl_field_name,
                    dataset_field_name = a.dataset_field_name,
                    //extraction_criteria = a.extraction_criteria,
                    default_value = a.default_value

                })
                .Join(dbContext.con_mst_tdataset_field
                .Where(df => df.dataset_code == ds1.target_dataset_code && df.delete_flag == "N"),
                    a => a.dataset_field_name, // Join key from con_trn_tpplfieldmapping
                    df => df.dataset_field_name,        // Join key from con_mst_tdatasetfield
                    (a, df) => new
                    {
                        a.pplfieldmapping_gid,
                        a.pipeline_code,
                        a.ppl_field_name,
                        a.dataset_field_name,
                        a.pplfieldmapping_flag,
                        //a.extraction_criteria,
                        a.default_value,
                        df.dataset_field_desc,
                        df.field_mandatory
                    }

                )
                .ToListAsync();

            try
            {
                if (ds_code == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(ds_code);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetpplFieldMappingList(string pipelincode, string dataset_code, string source_name)
        {
            var ds_code = await dbContext.con_mst_tdataset_field

                       .Where(a => a.dataset_code == dataset_code && a.delete_flag == "N")
                       .Select(a => new
                       {
                           dataset_field_desc = a.dataset_field_desc,
                           dataset_field_name = a.dataset_field_name
                       })
                       .ToListAsync();

            return Ok(ds_code);

        }

        [HttpPost]
        public async Task<IActionResult> Deletepplfieldmap(int id)
        {
            var pplfieldmap = await dbContext.con_trn_tpplfieldmapping.FindAsync(id);
            try
            {
                if (pplfieldmap != null)
                {
                    dbContext.Remove(pplfieldmap);
                    await dbContext.SaveChangesAsync();
                    return Ok("Deleted Successfully..!");
                }

                return NotFound("Not Found To Delete");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> Getpipelineinfo(int pipelinegid)
        {
            try
            {
                var pipeline = await dbContext.con_mst_tpipeline
                .Join(
                    dbContext.con_mst_tconnection,
                    a => a.connection_code,
                    b => b.connection_code,
                    (a, b) => new
                    {
                        pipeline_gid = a.pipeline_gid,
                        pipeline_code = a.pipeline_code,
                        pipeline_name = a.pipeline_name,
                        connection_code = a.connection_code,
                        connection_name = b.connection_name,
                        table_view_query_type = a.table_view_query_type,
                        table_view_query_desc = a.table_view_query_desc,
                        delete_flag = a.delete_flag,
                        bdelete_flag = b.delete_flag,
                    }
                )
                .Where(a => a.delete_flag == "N" && a.bdelete_flag == "N" && a.pipeline_gid == pipelinegid)
                .ToListAsync();
                return Ok(pipeline);
            }

            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetallExpressionsList(string pipeline_code)
        {
            var pplsrcexpr = await dbContext.con_trn_tpplsourcefield

                       .Where(a => a.pipeline_code == pipeline_code && a.source_type == "Expression" && a.delete_flag == "N")
                       .Select(a => new
                       {
                           pplsourcefield_gid = a.pplsourcefield_gid,
                           pipeline_code = a.pipeline_code,
                           sourcefield_name = a.sourcefield_name,
                           sourcefield_datatype = a.sourcefield_datatype,
                           sourcefield_expression = a.sourcefield_expression,
                           source_type = a.source_type
                       })
                       .ToListAsync();

            try
            {
                if (pplsrcexpr == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(pplsrcexpr);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }
        #endregion

        #region Pipeline Condition
        [HttpPost]
        public async Task<IActionResult> AddpplCondition([FromBody] AddpplConditionRequest addpplCondition)
        {
            try
            {

                int count = dbContext.con_trn_tpplcondition.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_trn_tpplcondition.Max(entity => entity.pplcondition_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }
                var pplcon = new PipelineCondition()
                {
                    pplcondition_gid = 0,//Guid.NewGuid(),
                    pipeline_code = addpplCondition.pipeline_code,
                    condition_type = addpplCondition.condition_type,
                    condition_name = addpplCondition.condition_name,
                    condition_text = addpplCondition.condition_text,
                    condition_msg = addpplCondition.condition_msg,
                    created_date = addpplCondition.created_date,
                    created_by = addpplCondition.created_by,
                    updated_date = addpplCondition.updated_date,
                    updated_by = addpplCondition.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_trn_tpplcondition.AddAsync(pplcon);
                await dbContext.SaveChangesAsync();

                var lastInsertedId = pplcon.pplcondition_gid;

                return Ok(lastInsertedId);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetpplCondition(string pipelinecode, string condition_type)
        {

            var ds_code = await dbContext.con_trn_tpplcondition
                          .Where(a => a.pipeline_code == pipelinecode && a.condition_type == condition_type && a.delete_flag == "N")
                          .Select(a => new
                          {
                              pplcondition_gid = a.pplcondition_gid,
                              pipeline_code = a.pipeline_code,
                              condition_type = a.condition_type,
                              condition_name = a.condition_name,
                              condition_text = a.condition_text,
                              condition_msg = a.condition_msg
                          })
                           .ToListAsync();
            try
            {
                if (ds_code == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(ds_code);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> Updatepplcondition([FromBody] UpdatepplConditionRequest updatepplCondition)
        {
            var pplcond = await dbContext.con_trn_tpplcondition.FindAsync(Convert.ToInt32(updatepplCondition.pplcondition_gid));
            try
            {

                if (pplcond != null)
                {
                    pplcond.condition_type = updatepplCondition.condition_type;
                    pplcond.condition_name = updatepplCondition.condition_name;
                    pplcond.condition_text = updatepplCondition.condition_text;
                    pplcond.condition_msg = updatepplCondition.condition_msg;
                    pplcond.updated_date = updatepplCondition.updated_date;
                    pplcond.updated_by = updatepplCondition.updated_by;

                    await dbContext.SaveChangesAsync();

                    return Ok("Record Updated Successfully");

                }
                return NotFound("Not Found TO Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public bool IsQueryValid(string conn_code, string db_name, string query)
        {
            var connector = dbContext.con_mst_tconnection
                 .Where(p => p.connection_code == conn_code && p.delete_flag == "N")
                 .Select(p => new ConnectionModel
                 {
                     source_host_name = p.source_host_name,
                     source_port = p.source_port,
                     source_db_user = p.source_db_user,
                     source_db_pwd = p.source_db_pwd,
                     source_db_type = p.source_db_type
                 })
                 .SingleOrDefault();
            try
            {
                if (connector == null)
                {
                    return false;
                }
                var connstring = "";

                if (connector.source_db_type == "MySql")
                {
                    connstring = "server=" + connector.source_host_name + "; Database=" + db_name + "; uid=" +
                             connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";

                    using (MySqlConnection connection = new MySqlConnection(connstring))
                    {
                        connection.Open();
                        query = query.Replace("[", "`").Replace("]", "`");
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                    return true;
                }
                else if (connector.source_db_type == "Postgres")
                {
                    connstring = "Host=" + connector.source_host_name + "; Database=" + db_name + "; Username=" +
                                 connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    using (NpgsqlConnection connection = new NpgsqlConnection(connstring))
                    {
                        connection.Open();
                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                    return true;
                }
                else if (connector.source_db_type == "Sql")
                {
                    using (SqlConnection connection = new SqlConnection(connstring))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                    return true;
                }

                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        [HttpGet]
        public string IsCheckQueryValid(string pipeline_code, string query, string query_for)
        {
            using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
            {
                if (connect.State != ConnectionState.Open)
                    connect.Open();

                try
                {
                    MySqlCommand command = connect.CreateCommand();

                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "pr_con_checkquery";

                    command.Parameters.AddWithValue("in_pipeline_code", pipeline_code);
                    command.Parameters.AddWithValue("in_query_for", query_for);
                    command.Parameters.AddWithValue("in_query", query);

                    MySqlParameter out_msg = new MySqlParameter("@out_msg", MySqlDbType.VarChar, 255)
                    {
                        Direction = ParameterDirection.Output
                    };
                    command.Parameters.Add(out_msg);
                    command.ExecuteNonQuery();

                    string outMsgValue = command.Parameters["@out_msg"].Value.ToString();
                    return outMsgValue;
                }

                catch (Exception ex)
                {
                    return ex.Message.ToString();
                }
            };

        }

        #endregion

        #region Pipeline Finalization
        [HttpPost]
        public async Task<IActionResult> Addpplfinalization([FromBody] AddpplFinalizationRequest addpplFinalization)
        {
            try
            {
                int count = dbContext.con_trn_tpplfinalization.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_trn_tpplfinalization.Max(entity => entity.finalization_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }
                if (addpplFinalization.extract_condition.ToString().Trim() != "")
                {
                    // Regular expression pattern to match key-value pairs
                    string pattern = @"\[(.*?)\] > \[(.*?)\]";

                    // Match key-value pairs using regular expression
                    MatchCollection matches = Regex.Matches(addpplFinalization.extract_condition, pattern);

                    // Create a list to hold JSON objects
                    List<JObject> jsonDataList = new List<JObject>();

                    foreach (Match match in matches)
                    {
                        // Extract key and value from the match groups
                        string key = match.Groups[1].Value;
                        string value = match.Groups[2].Value;

                        // Create a JObject for each pair
                        JObject jsonObject = new JObject();

                        // Add key-value pairs to the JObject
                        jsonObject.Add(key, value);

                        // Add the JObject to the list
                        jsonDataList.Add(jsonObject);
                    }

                    // Convert the list of JSON objects to a JSON array
                    JArray jsonArray = new JArray(jsonDataList);

                    // Convert the JSON array to a formatted string
                    addpplFinalization.last_incremental_val = jsonArray.ToString();
                }


                var pplfin = new PipelineFinalization()
                {
                    finalization_gid = 0,//Guid.NewGuid(),
                    pipeline_code = addpplFinalization.pipeline_code,
                    run_type = addpplFinalization.run_type,
                    cron_expression = addpplFinalization.cron_expression,
                    extract_mode = addpplFinalization.extract_mode,
                    upload_mode = addpplFinalization.upload_mode,
                    key_field = addpplFinalization.key_field,
                    extract_condition = addpplFinalization.extract_condition,
                    last_incremental_val = addpplFinalization.last_incremental_val,
                    pull_days = addpplFinalization.pull_days,
                    reject_duplicate_flag = addpplFinalization.reject_duplicate_flag,
                    error_mode = addpplFinalization.error_mode,
                    created_date = addpplFinalization.created_date,
                    created_by = addpplFinalization.created_by,
                    updated_date = addpplFinalization.updated_date,
                    updated_by = addpplFinalization.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_trn_tpplfinalization.AddAsync(pplfin);
                await dbContext.SaveChangesAsync();

                var lastInsertedId = pplfin.finalization_gid;

                //Update pipeine status
                var existingPipeline = await dbContext.con_mst_tpipeline.SingleOrDefaultAsync(p => p.pipeline_code == pplfin.pipeline_code);

                if (existingPipeline != null)
                {
                    // Update the properties of the existing entity
                    existingPipeline.pipeline_status = addpplFinalization.pipeline_status;

                    // Save the changes to the database
                    await dbContext.SaveChangesAsync();

                    //Insert on Scheduler table once pipeline activated

                    if (addpplFinalization.run_type == "Scheduled Run")
                    {
                        var schldpplcode = dbContext.con_trn_tscheduler
                         .Where(a => a.pipeline_code == pplfin.pipeline_code
                         //&& a.scheduler_status == "Scheduled" 
                         && a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked"
                         && a.delete_flag == "N")
                         .Select(a => new
                         {
                             scheduler_gid = a.scheduler_gid,
                             pipeline_code = a.pipeline_code,
                             Rawfilepath = a.file_path
                         }).OrderByDescending(a => a.scheduler_gid)
                         .FirstOrDefault();
                        if (schldpplcode != null)
                        {
                            var sch = new Scheduler()
                            {
                                scheduler_gid = 0,
                                scheduled_date = DateTime.Now,
                                pipeline_code = pplfin.pipeline_code,
                                file_name = src_filename,
                                scheduler_start_date = ReplaceTimeInCurrentDate(pplfin.cron_expression),//DateTime.Now,
                                scheduler_status = "Scheduled",
                                scheduler_initiated_by = pplfin.created_by,
                                delete_flag = "N"
                            };

                            await dbContext.con_trn_tscheduler.AddAsync(sch);
                            await dbContext.SaveChangesAsync();
                        }
                    }

                }

                return Ok("All details saved successfully..!");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> Updatepplfinalization([FromBody] UpdatepplFinalizationRequest updpplFinalization)
        {
            var pplfinz = await dbContext.con_trn_tpplfinalization.FindAsync(Convert.ToInt32(updpplFinalization.finalization_gid));
            try
            {
                string jsonString = "";
                if (updpplFinalization.extract_condition.ToString().Trim() != "")
                {
                    // Regular expression pattern to match key-value pairs
                    string pattern = @"\[(.*?)\] > \[(.*?)\]";

                    // Match key-value pairs using regular expression
                    MatchCollection matches = Regex.Matches(updpplFinalization.extract_condition, pattern);

                    // Create a list to hold JSON objects
                    List<JObject> jsonDataList = new List<JObject>();

                    foreach (Match match in matches)
                    {
                        // Extract key and value from the match groups
                        string key = match.Groups[1].Value;
                        string value = match.Groups[2].Value;

                        // Create a JObject for each pair
                        JObject jsonObject = new JObject();

                        // Add key-value pairs to the JObject
                        jsonObject.Add(key, value);

                        // Add the JObject to the list
                        jsonDataList.Add(jsonObject);
                    }

                    // Convert the list of JSON objects to a JSON array
                    JArray jsonArray = new JArray(jsonDataList);

                    // Convert the JSON array to a formatted string
                    jsonString = jsonArray.ToString();
                }

                if (pplfinz != null)
                {
                    pplfinz.run_type = updpplFinalization.run_type;
                    pplfinz.cron_expression = updpplFinalization.cron_expression;
                    pplfinz.extract_mode = updpplFinalization.extract_mode;
                    pplfinz.upload_mode = updpplFinalization.upload_mode;
                    pplfinz.key_field = updpplFinalization.key_field;
                    pplfinz.extract_condition = updpplFinalization.extract_condition;
                    pplfinz.last_incremental_val = jsonString;
                    pplfinz.pull_days = updpplFinalization.pull_days;
                    pplfinz.reject_duplicate_flag = updpplFinalization.reject_duplicate_flag;
                    pplfinz.error_mode = updpplFinalization.error_mode;
                    pplfinz.updated_date = updpplFinalization.updated_date;
                    pplfinz.updated_by = updpplFinalization.updated_by;

                    await dbContext.SaveChangesAsync();

                    //Get Pipelinecode
                    var pppl_code = dbContext.con_trn_tpplfinalization
                    .Where(p => p.finalization_gid == Convert.ToInt32(updpplFinalization.finalization_gid) && p.delete_flag == "N")
                    .Select(p => new PipelineFinalization
                    {
                        pipeline_code = p.pipeline_code
                    })
                    .SingleOrDefault();

                    //Update pipeine status
                    var existingPipeline = await dbContext.con_mst_tpipeline.SingleOrDefaultAsync(p => p.pipeline_code == pppl_code.pipeline_code);

                    if (existingPipeline != null)
                    {
                        // Update the properties of the existing entity
                        existingPipeline.pipeline_status = updpplFinalization.pipeline_status;

                        // Save the changes to the database
                        await dbContext.SaveChangesAsync();

                        if (updpplFinalization.run_type == "Scheduled Run")
                        {

                            var v_src_filename = "";

                            var pipelineWithConnector = await dbContext.con_mst_tpipeline
                            .Where(p => p.pipeline_code == pplfinz.pipeline_code && p.delete_flag == "N")
                            .Join(
                                dbContext.con_mst_tconnection,
                                 pipeline => pipeline.connection_code,
                                 connector => connector.connection_code,
                                (pipeline, connector) => new { Pipeline = pipeline, Connector = connector }
                            )
                            .FirstOrDefaultAsync();

                            v_src_filename = pipelineWithConnector.Pipeline.source_file_name;

                            if (pipelineWithConnector.Connector.source_db_type == "Excel")
                            {
                                v_filepath = v_filepath + v_src_filename;
                            }
                            else
                            {
                                v_filepath = "";
                            }

                            //Insert on Scheduler table once pipeline activated
                            var schldpplcode = dbContext.con_trn_tscheduler
                                 .Where(a => a.pipeline_code == pplfinz.pipeline_code
                                 //&& a.scheduler_status == "Scheduled" 
                                 && a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked"
                                 && a.delete_flag == "N")
                                 .Select(a => new
                                 {
                                     scheduler_gid = a.scheduler_gid,
                                     pipeline_code = a.pipeline_code,
                                     Rawfilepath = a.file_path
                                 }).OrderByDescending(a => a.scheduler_gid)
                                 .FirstOrDefault();
                            if (schldpplcode == null)
                            {
                                var sch = new Scheduler()
                                {
                                    scheduler_gid = 0,
                                    scheduled_date = DateTime.Now,
                                    pipeline_code = pplfinz.pipeline_code,
                                    file_path = v_filepath,
                                    file_name = v_src_filename,
                                    scheduler_start_date = ReplaceTimeInCurrentDate(pplfinz.cron_expression),//DateTime.Now,
                                    scheduler_status = "Scheduled",
                                    scheduler_initiated_by = pplfinz.created_by,
                                    delete_flag = "N"
                                };

                                await dbContext.con_trn_tscheduler.AddAsync(sch);
                                await dbContext.SaveChangesAsync();
                            }
                            else
                            {
                                var dlschedule = await dbContext.con_trn_tscheduler.FindAsync(schldpplcode.scheduler_gid);
                                dlschedule.scheduler_status = "Cancelled";
                                dlschedule.last_update_date = DateTime.Now;
                                dlschedule.scheduler_initiated_by = pplfinz.created_by;

                                await dbContext.SaveChangesAsync();
                                var sch = new Scheduler()
                                {
                                    scheduler_gid = 0,
                                    scheduled_date = DateTime.Now,
                                    pipeline_code = pplfinz.pipeline_code,
                                    file_path = v_filepath,
                                    file_name = v_src_filename,
                                    scheduler_start_date = ReplaceTimeInCurrentDate(pplfinz.cron_expression),//DateTime.Now,
                                    scheduler_status = "Scheduled",
                                    scheduler_initiated_by = pplfinz.created_by,
                                    delete_flag = "N"
                                };

                                await dbContext.con_trn_tscheduler.AddAsync(sch);
                                await dbContext.SaveChangesAsync();
                            }
                        }
                    }

                    return Ok("All details saved successfully..!");


                }
                return NotFound("Not Found To Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        public async Task<string> Reschedulefornexttime(string pipelinecode)
        {
            string msg = "Success";
            try
            {
                var finaliz = dbContext.con_trn_tpplfinalization
                         .Where(a => a.pipeline_code == pipelinecode && a.delete_flag == "N")
                         .Select(a => new
                         {
                             finalization_gid = a.finalization_gid,
                             cron_expression = a.cron_expression,
                             pipeline_code = a.pipeline_code
                         }).OrderByDescending(a => a.finalization_gid)
                         .FirstOrDefault();

                // var v_filepath = "D:\\Mohan\\ExcelScheduler\\";
                var v_src_filename = "";

                var pipelineWithConnector = await dbContext.con_mst_tpipeline
                .Where(p => p.pipeline_code == finaliz.pipeline_code && p.delete_flag == "N")
                .Join(
                    dbContext.con_mst_tconnection,
                                pipeline => pipeline.connection_code,
                    connector => connector.connection_code,
                    (pipeline, connector) => new { Pipeline = pipeline, Connector = connector }
                )
                .FirstOrDefaultAsync();

                v_src_filename = pipelineWithConnector.Pipeline.source_file_name;

                if (pipelineWithConnector.Connector.source_db_type == "Excel")
                {
                    v_filepath = v_filepath + v_src_filename;
                }
                else
                {
                    v_filepath = "";
                }


                //Insert on Scheduler table once pipeline activated
                var schldpplcode = dbContext.con_trn_tscheduler
                     .Where(a => a.pipeline_code == pipelinecode
                     //&& a.scheduler_status == "Scheduled" 
                     && a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked"
                     && a.delete_flag == "N")
                     .Select(a => new
                     {
                         scheduler_gid = a.scheduler_gid,
                         pipeline_code = a.pipeline_code,
                         Rawfilepath = a.file_path
                     }).OrderByDescending(a => a.scheduler_gid)
                     .FirstOrDefault();

                if (schldpplcode == null)
                {
                    DateTime v_scheduler_start_date = ReplaceTimeInCurrentDate(finaliz.cron_expression);
                    var sch = new Scheduler()
                    {
                        scheduler_gid = 0,
                        scheduled_date = DateTime.Now,
                        pipeline_code = pipelinecode,
                        file_path = v_filepath,
                        file_name = v_src_filename,
                        scheduler_start_date = v_scheduler_start_date,//DateTime.Now,
                        scheduler_status = "Scheduled",
                        scheduler_initiated_by = "System",
                        delete_flag = "N"
                    };

                    await dbContext.con_trn_tscheduler.AddAsync(sch);
                    await dbContext.SaveChangesAsync();
                }
                return msg;
            }
            catch (Exception ex)
            {
                msg = "Failed";
                return msg;
            }
        }

        public static DateTime ReplaceTimeInCurrentDate(string inputTime)
        {
            // Get the current date
            DateTime currentDateTime = DateTime.Now;

            // Parse the input time string to extract hours and minutes
            string[] timeComponents = inputTime.Split(':');
            int hours = int.Parse(timeComponents[0]);
            int minutes = int.Parse(timeComponents[1]);

            // Create a DateTime object with the given time and today's date
            DateTime givenDateTime = new DateTime(currentDateTime.Year, currentDateTime.Month, currentDateTime.Day, hours, minutes, 0);

            // Check if the given time has already passed today
            if (givenDateTime < currentDateTime)
            {
                // If so, add a day to the current date to get tomorrow's date
                givenDateTime = givenDateTime.AddDays(1);
            }

            return givenDateTime;

        }

        [HttpGet]
        public IActionResult GetSrcUpdattimestampFields(string connection_code, string databasename, string sourcetable)
        {
            var connector = dbContext.con_mst_tconnection
                .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                .Select(p => new ConnectionModel
                {
                    source_host_name = p.source_host_name,
                    source_port = p.source_port,
                    source_db_user = p.source_db_user,
                    source_db_pwd = p.source_db_pwd,
                    source_db_type = p.source_db_type,
                })
                .SingleOrDefault();

            try
            {
                if (connector == null)
                {
                    return NotFound("No Data found");
                }

                var src_connstring = "";

                List<SourcetblFields> columnList = new List<SourcetblFields>();

                if (connector.source_db_type == "MySql")
                {
                    src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (MySqlConnection connection = new MySqlConnection(src_connstring))
                    {
                        connection.Open();

                        string query = "SELECT COLUMN_NAME AS src_field_name, COLUMN_NAME AS src_field_desc " +
                                       "FROM information_schema.COLUMNS " +
                                       "WHERE TABLE_SCHEMA = @DatabaseName AND TABLE_NAME = @TableName AND DATA_TYPE = 'datetime'";

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", sourcetable);

                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("src_field_name"),
                                        Name = reader.GetString("src_field_desc")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                else if (connector.source_db_type == "Postgres")
                {
                    src_connstring = "Host=" + connector.source_host_name + "; Database=" + databasename + "; Username=" +
                                  connector.source_db_user + "; Password=" + connector.source_db_pwd + ";";
                    //Source connection establish
                    using (NpgsqlConnection connection = new NpgsqlConnection(src_connstring))
                    {
                        connection.Open();
                        string[] parts = sourcetable.Split('.');
                        databasename = parts[0];
                        sourcetable = parts[1];

                        string query = "SELECT COLUMN_NAME AS src_field_name, COLUMN_NAME AS src_field_desc " +
                                       "FROM information_schema.COLUMNS " +
                                       "WHERE TABLE_SCHEMA = @DatabaseName AND TABLE_NAME = @TableName AND DATA_TYPE = 'timestamp without time zone'";

                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DatabaseName", databasename);
                            command.Parameters.AddWithValue("@TableName", sourcetable);

                            using (NpgsqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    SourcetblFields column = new SourcetblFields
                                    {
                                        ID = reader.GetString("src_field_name"),
                                        Name = reader.GetString("src_field_desc")
                                    };
                                    columnList.Add(column);
                                }
                            }
                        }
                    }
                }
                return Ok(columnList);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetpplFinalization(string pipelinecode)
        {

            var pplfinz = await dbContext.con_trn_tpplfinalization

                       .Where(a => a.pipeline_code == pipelinecode && a.delete_flag == "N")
                       .Select(a => new
                       {
                           finalization_gid = a.finalization_gid,
                           pipeline_code = a.pipeline_code,
                           run_type = a.run_type,
                           cron_expression = a.cron_expression,
                           extract_mode = a.extract_mode,
                           upload_mode = a.upload_mode,
                           key_field = a.key_field,
                           extract_condition = a.extract_condition,
                           pull_days = a.pull_days,
                           reject_duplicate_flag = a.reject_duplicate_flag,
                           error_mode = a.error_mode
                       })
                       .ToListAsync();

            try
            {
                if (pplfinz == null)
                {
                    return NotFound("Not Found");
                }

                return Ok(pplfinz);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetRuntypeScheduledList()
        {
            try
            {
                // Get a list of active pipelines
                var activePipelines = dbContext.con_mst_tpipeline
                    .Where(p => p.pipeline_status == "Active" && p.delete_flag == "N")
                    .Select(c => c.pipeline_code)
                    .ToList();

                // Get scheduled runs for active pipelines
                var scheduledRuns = dbContext.con_trn_tpplfinalization
                    .Where(p => p.run_type == "Scheduled Run"
                                && activePipelines.Contains(p.pipeline_code)
                                && p.delete_flag == "N")
                    .Select(c => new
                    {
                        cron_expression = c.cron_expression,
                        pipeline_code = c.pipeline_code,
                        delete_flag = c.delete_flag
                    })
                    .ToList();

                return Ok(scheduledRuns);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public string SetPipelineStatus(string validation_type, string dscode, string ppl_code)
        {
            var status = "Draft";
            var count = 0;
            if (validation_type == "FieldMapping")
            {
                if (dscode != null)
                {

                    count = dbContext.con_trn_tpplfieldmapping
                        .Where(x =>
                            x.dataset_code == dscode &&
                            x.delete_flag == "N" &&
                            x.pplfieldmapping_flag == 1 &&
                            //x.ppl_field_name == "-- Select --" &&
                            x.pipeline_code == ppl_code)
                        .Count();


                    if (count == 0)
                    {
                        var count2 = dbContext.con_trn_tpplfieldmapping
                       .Where(x =>
                           x.dataset_code == dscode &&
                           x.delete_flag == "N" &&
                           x.pplfieldmapping_flag == 0 &&
                           x.ppl_field_name != "-- Select --" &&
                           x.pipeline_code == ppl_code)
                       .Count();

                        if (count2 <= 0)
                        {
                            status = "Draft";
                        }
                        else
                        {
                            status = "Active";
                        }
                    }
                    else
                    {
                        var count3 = dbContext.con_trn_tpplfieldmapping
                        .Where(x =>
                            x.dataset_code == dscode &&
                            x.delete_flag == "N" &&
                            x.pplfieldmapping_flag == 1 &&
                            x.ppl_field_name == "-- Select --" &&
                            x.pipeline_code == ppl_code)
                        .Count();

                        if (count3 == 0)
                        {
                            status = "Active";
                        }

                    }
                }
            }

            return status;
        }

        [HttpGet]
        public async Task<IActionResult> ExtractCondition_validation(string connection_code, string databasename, string targettable,
                                                         string pipeline_code, string extract_cond_for, string query)
        {
            var ppl = dbContext.con_mst_tpipeline
           .Where(p => p.pipeline_code == pipeline_code && p.pipeline_status == "Active" && p.delete_flag == "N")
           .Select(p => new Pipeline
           {
               connection_code = p.connection_code,
               table_view_query_desc = p.table_view_query_desc,
               db_name = p.db_name
           })
           .SingleOrDefault();

            var connector = dbContext.con_mst_tconnection
                .Where(p => p.connection_code == connection_code && p.delete_flag == "N")
                .Select(p => new ConnectionModel
                {
                    source_host_name = p.source_host_name,
                    source_port = p.source_port,
                    source_db_user = p.source_db_user,
                    source_db_pwd = p.source_db_pwd,
                    source_db_type = p.source_db_type,
                })
                .SingleOrDefault();

            try
            {
                string outMsgValue = "";
                if (connector.ToString() == "")
                {
                    outMsgValue = "No Data found";
                }

                var src_connstring = "";

                if (connector.source_db_type == "MySql")
                {
                    src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + "; database=" + ppl.db_name + ";";
                    //Source connection establish
                    using (MySqlConnection connection = new MySqlConnection(src_connstring))
                    {
                        connection.Open();

                        string query1 = "SELECT * FROM " + targettable +
                                       " WHERE " + query;

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.CommandText = "pr_con_checkquery_extractcond";
                            command.CommandTimeout = 0;
                            command.Parameters.AddWithValue("in_db_name", databasename);
                            command.Parameters.AddWithValue("in_target_table_name", targettable);
                            command.Parameters.AddWithValue("in_pipeline_code", pipeline_code);
                            command.Parameters.AddWithValue("in_extract_condition_for", extract_cond_for);
                            command.Parameters.AddWithValue("in_query", query1);
                            MySqlParameter out_msg = new MySqlParameter("@out_msg", MySqlDbType.VarChar, 255)
                            {
                                Direction = ParameterDirection.Output
                            };
                            command.Parameters.Add(out_msg);

                            command.ExecuteNonQuery();
                            outMsgValue = command.Parameters["@out_msg"].Value.ToString();

                            connection.Close();

                        }
                    }
                }
                return Ok(outMsgValue);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        #endregion

        #region Connection String
        //SQL Connection string
        private SqlConnection GetSqlServerConnection(string host_name, string db_name, string user_name, string pswd)
        {
            string connectionString = "DataSource =" + host_name + ",InitialCatalog =" + db_name + ",UserID =" + user_name + ",Password =" + pswd + ";";
            SqlConnection connection = new SqlConnection(connectionString);
            return connection;
        }

        //MYSQL Connection string
        private MySqlConnection GetMySqlServerConnection(string host_name, string db_name, string user_name, string pswd)
        {
            string connectionString = "server=" + host_name + "; Database =" + db_name + "; Uid =" + user_name + "; Pwd =" + pswd + ";";
            MySqlConnection connection = new MySqlConnection(connectionString);
            return connection;
        }

        //Postgres Connection string
        private NpgsqlConnection GetPostgresServerConnection(string host_name, string db_name, string user_name, string pswd)
        {
            string connectionString = "Host=" + host_name + "; Database =" + db_name + "; Username =" + user_name + "; Password =" + pswd + ";";
            NpgsqlConnection connection = new NpgsqlConnection(connectionString);
            return connection;
        }
        #endregion

        #region GetDatatable Dynamically
        private DataTable GetDataTableFromMySQLServer(MySqlConnection connstring, string table_name, string column_name,
            string defaultvalue, string updated_time_stamp, string pull_days)
        {
            var conn = connstring;

            string[] elements = column_name.Split(',');
            string[] dfvalue = defaultvalue.Split(',');
            string column_name_cond = "";

            for (var i = 0; i < elements.Length; i++)
            {
                string element = elements[i];  // Retrieve the current element using the index
                string defaultval = dfvalue[i];
                string v_defaultval = "";

                // Default condition 
                if (defaultval.Trim() == "" || defaultval == null)
                {
                    v_defaultval = element;
                }
                else
                {
                    v_defaultval = defaultval;
                }

                string element_cond = " CASE WHEN " + element + " IS NULL OR " + element + " = '' THEN " + v_defaultval + " ELSE " + element + " END as " + element + ",";
                column_name_cond += element_cond;
            }

            // Remove the trailing comma from the last element
            column_name_cond = column_name_cond.TrimEnd(',');

            string query = "SELECT " + column_name_cond + " FROM " + table_name + ";";

            var command = new MySqlCommand(query, conn);
            conn.Open();
            var reader = command.ExecuteReader();
            DataTable table = new DataTable();
            table.Load(reader);
            conn.Close();

            return table;
        }

        private DataTable GetDataTableFromPostgreSQLServer(NpgsqlConnection connstring, string table_name, string column_name, string defaultvalue)
        {
            var conn = connstring;

            string[] elements = column_name.Split(',');
            string[] dfvalue = defaultvalue.Split(',');
            string column_name_cond = "";

            for (var i = 0; i < elements.Length; i++)
            {
                string element = elements[i];
                string defaultval = dfvalue[i].Trim();
                string v_defaultval = "";
                if (string.IsNullOrWhiteSpace(defaultval))
                {
                    v_defaultval = element;
                }
                else
                {
                    v_defaultval = "'" + defaultval + "'";
                }
                string element_cond = $"CASE WHEN {element} IS NULL THEN {v_defaultval} ELSE {element} END AS {element},";
                column_name_cond += element_cond;
            }

            // Remove the trailing comma from the last element
            column_name_cond = column_name_cond.TrimEnd(',');

            string query = $"SELECT {column_name_cond} FROM {table_name};";

            var command = new NpgsqlCommand(query, conn);
            conn.Open();
            var reader = command.ExecuteReader();
            DataTable table = new DataTable();
            table.Load(reader);
            conn.Close();

            return table;
        }
        #endregion

        #region PushData into Target Table
        private void PushDataToMySQL(string source_type, DataTable table, string target_table, string upload_mode, string primary_key)
        {
            try
            {
                var connection = GetMySqlServerConnection(trg_hstname, trg_dbname, trg_username, trg_password);
                connection.Open();
                if (upload_mode == "Insert or Update based on key")
                {
                    //using (MySqlCommand command = connection.CreateCommand())
                    //{
                    //    command.CommandText = "CREATE UNIQUE INDEX IX_UniqueIndexName ON " + target_table + "(" + primary_key + "); ";
                    //    command.ExecuteNonQuery();
                    //}
                    if (source_type == "Postgres")
                    {
                        string[] parts = target_table.Split('.');
                        target_table = parts[1];
                    }
                    else
                    {
                        target_table = target_table.Trim();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        string query = ConstructInsertUpdateQuery(source_type, target_table, table.Columns, row, primary_key);

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            foreach (DataColumn column in table.Columns)
                            {
                                command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
                            }

                            command.ExecuteNonQuery();
                        }
                    }

                    //using (MySqlCommand command = connection.CreateCommand())
                    //{
                    //    command.CommandText = "DROP INDEX IX_UniqueIndexName ON " + target_table + ";";
                    //    command.ExecuteNonQuery();
                    //}

                }
                else if (upload_mode == "Clean and Insert based on Primary key")
                {
                    if (source_type == "Postgres")
                    {
                        string[] parts = target_table.Split('.');
                        target_table = parts[1];
                    }
                    else
                    {
                        target_table = target_table.Trim();
                    }
                    using (MySqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "TRUNCATE TABLE " + target_table + ";";
                        command.ExecuteNonQuery();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        string query = ConstructInsertUpdateQuery(source_type, target_table, table.Columns, row, primary_key);

                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            foreach (DataColumn column in table.Columns)
                            {
                                command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
                            }

                            command.ExecuteNonQuery();
                        }

                    }
                }

                connection.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex);
            }
        }

        #endregion

        #region Build The Insert/Update Query
        public static string ConstructInsertUpdateQuery(string source_dbtype, string tableName, DataColumnCollection columns, DataRow row, string primary_key)
        {
            //Step 1
            //string columnNames = string.Join(", ", columns.Cast<DataColumn>().Select(c => c.ColumnName));
            //string parameterNames = string.Join(", ", columns.Cast<DataColumn>().Select(c => $"@{c.ColumnName}"));
            //return $"INSERT INTO {tableName} ({columnNames}) VALUES ({parameterNames})";

            //Step 2

            var primaryKeyColumnName = primary_key; // Replace with your actual primary key column name

            string columnNames = string.Join(", ", columns.Cast<DataColumn>().Select(c => c.ColumnName));
            string parameterNames = string.Join(", ", columns.Cast<DataColumn>().Select(c => $"@{c.ColumnName}"));
            string query = "";
            //if (source_dbtype == "Mysql")
            //{
            var updateColumns = columns.Cast<DataColumn>()
                                    .Where(c => c.ColumnName != primaryKeyColumnName)
                                    .Select(c => $"{c.ColumnName} = VALUES({c.ColumnName})");

            query = $@" INSERT INTO {tableName} ({columnNames}) 
                            VALUES ({parameterNames}) 
                            ON DUPLICATE KEY UPDATE 
                            {string.Join(", ", updateColumns)};";
            //}
            //else if (source_dbtype == "Postgres")
            //{
            //    var updateColumns = columns.Cast<DataColumn>()
            //                .Where(c => c.ColumnName != primaryKeyColumnName)
            //                .Select(c => $"{c.ColumnName} = EXCLUDED.{c.ColumnName}");

            //    query = $@" INSERT INTO {tableName} ({columnNames}) 
            //                VALUES ({parameterNames}) 
            //                ON CONFLICT ({primaryKeyColumnName}) DO UPDATE
            //                SET {string.Join(", ", updateColumns)};";
            //}
            return query;

        }
        #endregion

        #region Excel Data push
        //public string Exceldatapush(string pipeline_code, string Rawfilepath)
        public async Task<string> Exceldatapush(int scheduler_gid)
        {
            string msg = "";
            DataSet dataSet = null;

            try
            {
                //Get Pipeline codeagainst scheduler id
                var schldpplcode = dbContext.con_trn_tscheduler
                          .Where(a => a.scheduler_gid == scheduler_gid
                                 //&& a.scheduler_status == "Scheduled"
                                 && a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked"

                          && a.delete_flag == "N")
                          .Select(a => new
                          {
                              scheduler_gid = a.scheduler_gid,
                              pipeline_code = a.pipeline_code,
                              Rawfilepath = a.file_path
                          }).OrderByDescending(a => a.scheduler_gid)
                          .FirstOrDefault();

                if (schldpplcode != null)
                {

                    // Call the FieldmappingDT method
                    DataTable dataTable = FieldmappingDT(schldpplcode.pipeline_code).Result;
                    if (dataTable.Rows.Count <= 0)
                    {
                        UpdateScheduler(scheduler_gid, "Failed", "System");
                        return "Fieldmapping is not done for this pipeline...";
                    }

                    sched_gid = scheduler_gid;

                    //Get dataset 
                    dataSet = ExcelToDataSet(schldpplcode.pipeline_code, schldpplcode.Rawfilepath);

                    int dtrow_count = dataTable.Rows.Count;
                    //int matched_count = 0;

                    //for (int m = 0; dataSet.Tables[0].Columns.Count > m; m++)


                    //var expressionNames = dbContext.con_trn_tpplsourcefield
                    // .Where(p => p.pipeline_code == schldpplcode.pipeline_code
                    //         && (p.source_type == "Expression")
                    //         && p.delete_flag == "N")
                    // .Select(a => new
                    // {
                    //     sourcefield_name = a.sourcefield_name,
                    //     sourcefield_expression = a.sourcefield_expression
                    // })
                    // .ToList();

                    //if (expressionNames.Count > 0)
                    //{
                    //    for (int i = 0; i < expressionNames.Count; i++)
                    //    {
                    //        dataSet.Tables[0].Columns.Add(expressionNames[i].sourcefield_name);
                    //    }
                    //}

                    msg = DatatableToCSV(dataSet.Tables[0], schldpplcode.pipeline_code);
                }
                else
                {

                    msg = "This Pipeline is not scheduled..!";
                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = scheduler_gid,
                    in_errorlog_type = "Catch - Method Name : Exceldatapush",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog);
                UpdateScheduler(scheduler_gid, "Failed", "System");
                return "Error: " + ex.Message;
            }

            return msg;
        }

        [HttpPost]
        public async Task<DataTable> FieldmappingDT(string pipelinecode)
        {
            DataTable dataTable = new DataTable();

            try
            {
                var ds1 = dbContext.con_mst_tpipeline
             .Where(p => p.pipeline_code == pipelinecode && p.delete_flag == "N")
             .Select(p => new Pipeline
             {
                 target_dataset_code = p.target_dataset_code,
             })
             .SingleOrDefault();

                var ds_code = await dbContext.con_trn_tpplsourcefield
                    .Where(a => a.pipeline_code == pipelinecode
                    && a.source_type != "Expression"
                    && a.delete_flag == "N")
                    .Select(a => new
                    {
                        //dataset_field_name = a.dataset_field_name,
                        ppl_field_name = a.sourcefield_name
                    }).ToListAsync();

                // Define the columns in the DataTable
                dataTable.Columns.Add("ppl_field_name");
                dataTable.Columns.Add("default_value");

                // Populate the DataTable with data from the query
                foreach (var item in ds_code)
                {
                    DataRow row = dataTable.NewRow();
                    row["ppl_field_name"] = item.ppl_field_name;
                    //row["default_value"] = item.default_value;
                    dataTable.Rows.Add(row);
                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : FieldmappingDT",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog);
                // Handle any exceptions here
                UpdateScheduler(sched_gid, "Failed", "System");
            }
            return dataTable;
        }

        [HttpPost]
        public DataSet ExcelToDataSet(string pipelinecode, string rawFilePath)
        {
            string fileExtension = "";
            string query = "";
            string[] parts = rawFilePath.Split('.');
            fileExtension = "." + parts.Last();
            DataSet ds = new DataSet();
            string dataset_code = "";
            ppl_code = pipelinecode;
            try
            {
                var query1 = "";

                //Getting mapped columns srcfieldname,srctype,slno from the rawexcel
                var bcpcolumns = (from a in dbContext.con_trn_tpplsourcefield
                                  where a.pipeline_code == pipelinecode
                                  where (a.sourcefieldmapping_flag == "Y" || a.source_type == "Expression")
                                  where a.delete_flag == "N"
                                  orderby a.dataset_table_field_sno
                                  select new
                                  {
                                      a.sourcefield_sno,
                                      a.sourcefield_name,
                                      a.sourcefield_datatype,
                                      a.dataset_table_field,
                                      a.source_type
                                  }).ToList(); // Execute query immediately

                //Getting Src Column from  con_trn_tpplsourcefield
                var sourcecolumns = (from a in dbContext.con_trn_tpplsourcefield
                                     where a.pipeline_code == pipelinecode
                                     where a.source_type != "Expression"
                                     where a.delete_flag == "N"
                                     orderby a.sourcefield_sno
                                     select new
                                     {
                                         a.sourcefield_name,
                                         a.sourcefield_sno
                                     }).ToList();

                var ppl_dscode = dbContext.con_mst_tpipeline
                                   .Where(p => p.pipeline_code == pipelinecode && p.pipeline_status == "Active" && p.delete_flag == "N")
                                   .Select(a => new
                                   {
                                       a.target_dataset_code
                                   }).FirstOrDefault();
                dataset_code = ppl_dscode?.target_dataset_code;

                // Inclusion condition Apply
                var filtercond = dbContext.con_trn_tpplcondition
                                 .Where(p => p.pipeline_code == pipelinecode
                                             && (p.condition_type == "Filter")
                                             && p.delete_flag == "N")
                                 .Select(a => new
                                 {
                                     condition_text = a.condition_text
                                 }).ToList();

                if (filtercond.Any() && !string.IsNullOrEmpty(filtercond[0].condition_text))
                {
                    query1 = " and (" + filtercond[0].condition_text + ")";
                }

                // Exclusion condition Apply
                var rejectioncond = dbContext.con_trn_tpplcondition
                                   .Where(p => p.pipeline_code == pipelinecode
                                               && (p.condition_type == "Rejection")
                                               && p.delete_flag == "N")
                                   .Select(a => new
                                   {
                                       condition_text = a.condition_text
                                   }).ToList();

                bool mdf_flag = false;

                if (rejectioncond.Any() && !string.IsNullOrEmpty(rejectioncond[0].condition_text) && !mdf_flag)
                {
                    string modifiedCondition = rejectioncond[0].condition_text;
                    if (rejectioncond[0].condition_text.Contains("="))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("=", "<>");
                    }
                    else if (rejectioncond[0].condition_text.Contains(">"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace(">", "<");
                    }
                    else if (rejectioncond[0].condition_text.Contains("<"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("<", ">");
                    }

                    if (!string.IsNullOrEmpty(modifiedCondition))
                    {
                        query1 += " and (" + modifiedCondition + ")";
                        mdf_flag = true;
                    }
                }

                string excelConnectionString = "";

                if (fileExtension == ".xls" || fileExtension == ".xlsx")
                {

                    using (var workbook = new XLWorkbook(rawFilePath))
                    {
                        var worksheet = workbook.Worksheets.FirstOrDefault();
                        if (worksheet != null)
                        {
                            // column header validation
                            foreach (var items in sourcecolumns)
                            {
                                if (worksheet.Cell(1, items.sourcefield_sno).GetValue<string>().ToLower() != items.sourcefield_name.ToLower())
                                {
                                    throw new Exception("File Header Name Missmatch !");
                                }
                            }

                            DataTable dt = new DataTable();
                            int totalRows = worksheet.LastRowUsed().RowNumber();
                            int totalCols = bcpcolumns.Count;//worksheet.Dimension.End.Column;

                            string[,] colType = new string[totalCols, 3];
                            int i = 0;

                            foreach (var items in bcpcolumns)
                            {
                                dt.Columns.Add(items.sourcefield_name);
                                var trgds_datatype = dbContext.con_col_datatype
                                    .Where(p => p.sourcefield_name == items.sourcefield_name 
                                        && p.pipeline_code == pipelinecode
                                        && p.dataset_code == dataset_code)
                                    .Select(a => new
                                    {
                                        a.dataset_code,
                                        a.dataset_field_type,
                                        a.sourcefield_name,
                                        a.dataset_field_name,
                                        a.source_type
                                    }).FirstOrDefault();

                                colType[i, 0] = items.sourcefield_name;
                                colType[i, 1] = items.sourcefield_sno.ToString();

                                if (trgds_datatype != null)
                                {
                                    if (trgds_datatype.source_type.ToUpper() != "EXPRESSION")
                                        colType[i, 2] = trgds_datatype?.dataset_field_type ?? "TEXT";
                                    else
                                        colType[i, 2] = "EXPRESSION";
                                }
                                else
                                {
                                    //colType[i, 2] = "EXPRESSION";
                                    colType[i, 2] = "TEXT";
                                }

                                i++;
                            }

                            int col = 0;
                            i = 0;

                            for (int row = 2; row <= totalRows; row++)
                            {
                                DataRow newRow = dt.NewRow();
                                object cellValue = null;

                                for (i = 0; i < totalCols; i++)
                                {
                                    col = Convert.ToInt16(colType[i, 1]);
                                    string columnName = colType[i, 0];
                                    string colDataType = colType[i, 2];

                                    if (colDataType != "EXPRESSION")
                                    {
                                        cellValue = worksheet.Cell(row, col).Value; // Retrieve cell value
                                    }
                                    else
                                    {
                                        colDataType = "TEXT";
                                        cellValue = "";
                                    }

                                    if (colDataType == "TEXT")
                                    {
                                        newRow[i] = string.IsNullOrEmpty(cellValue?.ToString()) ? "" : cellValue.ToString();
                                    }
                                    else if (colDataType == "DATE")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double excelSerialDate))
                                        {
                                            DateTime dateValue = DateTime.FromOADate(excelSerialDate);
                                            newRow[i] = dateValue.ToString("yyyy-MM-dd");
                                        }
                                        else if (DateTime.TryParse(cellValue?.ToString(), out DateTime dateTimeValue))
                                        {
                                            newRow[i] = dateTimeValue.ToString("yyyy-MM-dd");
                                        }
                                        else if (DateTime.TryParseExact(cellValue?.ToString(), "dd/MM/yyyy", null, System.Globalization.DateTimeStyles.None, out DateTime exactDateTimeValue))
                                        {
                                            newRow[i] = exactDateTimeValue.ToString("yyyy-MM-dd");
                                        }
                                        else
                                        {
                                            newRow[i] = ""; // Set to empty string if cellValue is null, empty, or invalid
                                        }
                                    }
                                    else if (colDataType == "DATETIME")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double excelSerialDate))
                                        {
                                            DateTime dateValue = DateTime.FromOADate(excelSerialDate);
                                            newRow[i] = dateValue.ToString("yyyy-MM-dd HH:mm:ss");
                                        }
                                        else if (DateTime.TryParse(cellValue?.ToString(), out DateTime dateTimeValue))
                                        {
                                            newRow[i] = dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss");
                                        }
                                        else
                                        {
                                            newRow[i] = ""; // Set to empty string if cellValue is null, empty, or invalid
                                        }
                                    }
                                    else if (colDataType == "NUMERIC")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double numericValue))
                                        {
                                            newRow[i] = numericValue.ToString();
                                        }
                                        else
                                        {
                                            newRow[i] = "0";
                                        }
                                    }
                                    else if (colDataType == "INTEGER")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double numericValue))
                                        {
                                            newRow[i] = numericValue.ToString();
                                        }
                                        else
                                        {
                                            newRow[i] = "0";
                                        }
                                    }
                                }
                                dt.Rows.Add(newRow);
                            }

                            var filteredRows = dt.Select("1 = 1 " + query1);
                            DataTable filteredDt = filteredRows.Any() ? filteredRows.CopyToDataTable() : dt.Clone();

                            ds.Tables.Add(filteredDt);
                        }
                    }
                }
                else
                {
                    // Handle unsupported file types
                }

                string directory = System.IO.Path.GetDirectoryName(rawFilePath); // Extract directory path

                if (directory == v_filepath.TrimEnd('\\'))
                {
                    MovetheProcessedfile(rawFilePath, comp_file_path, sched_gid.ToString());
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            return ds;
        }


        [HttpPost]
        public DataSet ExcelToDataSet_08072024(string pipelinecode, string rawFilePath)
        {
            string fileExtension = "";
            string query = "";
            string[] parts = rawFilePath.Split('.');
            fileExtension = "." + parts.Last();
            string result = "";
            DataSet ds = new DataSet();
            string dataset_code = "";
            ppl_code = pipelinecode;
            try
            {
                var query1 = "";

                //Getting mapped columns srcfieldname,srctype,slno from the rawexcel
                var bcpcolumns = from a in dbContext.con_trn_tpplsourcefield
                                 where a.pipeline_code == pipelinecode
                                 where (a.sourcefieldmapping_flag == "Y" || a.source_type == "Expression")
                                 where a.delete_flag == "N"
                                 orderby a.dataset_table_field_sno
                                 select new
                                 {
                                     a.sourcefield_sno,
                                     a.sourcefield_name,
                                     a.sourcefield_datatype,
                                     a.dataset_table_field,
                                     a.source_type
                                 };
                var resultList = bcpcolumns.ToList();


                var ppl_dscode = dbContext.con_mst_tpipeline
                                   .Where(p => p.pipeline_code == pipelinecode && p.pipeline_status == "Active" && p.delete_flag == "N")
                                   .Select(a => new
                                   {
                                       a.target_dataset_code
                                   }).FirstOrDefault();
                dataset_code = ppl_dscode.target_dataset_code;


                // Inclusion condition Apply
                var filtercond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Filter")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();

                if (filtercond.Count > 0)
                {
                    if (!string.IsNullOrEmpty(filtercond[0].condition_text))
                    {
                        query1 = " and (" + filtercond[0].condition_text + ")";
                    }
                }

                // Exclusion condition Apply
                var rejectioncond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Rejection")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();

                bool mdf_flag = false;

                if (rejectioncond.Count > 0 && rejectioncond[0].condition_text != "" && mdf_flag == false)
                {
                    string modifiedCondition = "";

                    if (rejectioncond[0].condition_text.Contains("="))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("=", "<>");
                    }
                    else if (rejectioncond[0].condition_text.Contains(">"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace(">", "<");
                    }
                    else if (rejectioncond[0].condition_text.Contains("<"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("<", ">");
                    }

                    if (!string.IsNullOrEmpty(modifiedCondition))
                    {
                        query1 += " and (" + modifiedCondition + ")";
                        mdf_flag = true;
                    }
                }

                string excelConnectionString = "";

                if (fileExtension == ".xls" || fileExtension == ".xlsx")
                {

                    using (var workbook = new XLWorkbook(rawFilePath))
                    {
                        var worksheet = workbook.Worksheets.FirstOrDefault();
                        if (worksheet != null)
                        {
                            DataTable dt = new DataTable();
                            int totalRows = worksheet.LastRowUsed().RowNumber();
                            int totalCols = worksheet.LastColumnUsed().ColumnNumber();

                            string[,] colType = new string[2, totalCols];
                            int i = 0;
                            string colDataType;


                            foreach (var items in bcpcolumns)
                            {
                                dt.Columns.Add(items.sourcefield_name);

                                var trgds_datatype = dbContext.con_col_datatype
                                    .Where(p => p.sourcefield_name == items.sourcefield_name && p.dataset_code == dataset_code)
                                    .Select(a => new
                                    {
                                        a.dataset_code,
                                        a.dataset_field_type,
                                        a.sourcefield_name,
                                        a.dataset_field_name
                                    }).FirstOrDefault();

                                colType[0, i] = items.sourcefield_name;
                                colType[1, i++] = trgds_datatype?.dataset_field_type ?? "TEXT";
                            }


                            for (int row = 2; row <= totalRows; row++)
                            {
                                DataRow newRow = dt.NewRow();
                                for (int col = 1; col <= totalCols; col++)
                                {
                                    object cellValue = worksheet.Cell(row, col).Value;// Retrieve cell value
                                    string columnName = dt.Columns[col - 1].ColumnName;

                                    colDataType = colType[1, col - 1];

                                    if (colDataType == "TEXT")
                                    {
                                        newRow[col - 1] = string.IsNullOrEmpty(cellValue?.ToString()) ? "" : cellValue.ToString();
                                    }
                                    else if (colDataType == "DATE")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double excelSerialDate))
                                        {
                                            DateTime dateValue = DateTime.FromOADate(excelSerialDate);
                                            newRow[col - 1] = dateValue.ToString("yyyy-MM-dd");
                                        }
                                        else if (DateTime.TryParse(cellValue?.ToString(), out DateTime dateTimeValue))
                                        {
                                            newRow[col - 1] = dateTimeValue.ToString("yyyy-MM-dd");
                                        }
                                        else if (DateTime.TryParseExact(cellValue?.ToString(), "dd/MM/yyyy", null, System.Globalization.DateTimeStyles.None, out DateTime exactDateTimeValue))
                                        {
                                            newRow[col - 1] = exactDateTimeValue.ToString("yyyy-MM-dd");
                                        }
                                        else
                                        {
                                            newRow[col - 1] = ""; // Set to empty string if cellValue is null, empty, or invalid
                                        }

                                    }
                                    else if (colDataType == "DATETIME")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double excelSerialDate))
                                        {
                                            DateTime dateValue = DateTime.FromOADate(excelSerialDate);
                                            newRow[col - 1] = dateValue.ToString("yyyy-MM-dd HH:mm:ss");
                                        }
                                        else if (DateTime.TryParse(cellValue?.ToString(), out DateTime dateTimeValue))
                                        {
                                            newRow[col - 1] = dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss");
                                        }
                                        else
                                        {
                                            newRow[col - 1] = ""; // Set to empty string if cellValue is null, empty, or invalid
                                        }

                                    }
                                    else if (colDataType == "NUMERIC")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double numericValue))
                                        {
                                            newRow[col - 1] = numericValue.ToString();
                                        }
                                        else
                                        {
                                            newRow[col - 1] = "0";
                                        }
                                    }
                                    else if (colDataType == "INTEGER")
                                    {
                                        if (double.TryParse(cellValue?.ToString(), out double numericValue))
                                        {
                                            newRow[col - 1] = numericValue.ToString();
                                        }
                                        else
                                        {
                                            newRow[col - 1] = "0";
                                        }
                                    }

                                }
                                dt.Rows.Add(newRow);
                            }

                            var z = dt.Rows.Count;

                            var filteredRows = dt.Select("1 = 1 " + query1);
                            DataTable filteredDt = filteredRows.Any() ? filteredRows.CopyToDataTable() : dt.Clone();

                            ds.Tables.Add(filteredDt);

                        }
                    }
                }
                else
                {
                    // Handle unsupported file types
                }

                string directory = System.IO.Path.GetDirectoryName(rawFilePath); // Extract directory path

                if (directory == v_filepath.TrimEnd('\\'))
                {
                    MovetheProcessedfile(rawFilePath, comp_file_path, sched_gid.ToString());
                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : ExcelToDataSet",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog);
                UpdateScheduler(sched_gid, "Failed", "System");
                string errormsg = "Error: " + ex.Message;
            }
            return ds;
        }

        [HttpPost]
        public DataSet ExcelToDataSet_OLEDB(string pipelinecode, string rawFilePath)
        {
            string fileExtension = "";
            string query = "";
            string[] parts = rawFilePath.Split('.');
            fileExtension = "." + parts.Last();
            string result = "";
            DataSet ds = new DataSet();
            try
            {
                var query1 = "";
                //errormsg = "Step 8 : " + rawFilePath;
                //System.IO.File.AppendAllText(errorlogfilePath, errormsg + Environment.NewLine);
                //Inclusion condition Apply
                var filtercond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Filter")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();

                if (filtercond.Count > 0)
                {

                    if (filtercond[0].condition_text != "")
                    {
                        query1 = "and (" + filtercond[0].condition_text + ")";
                    }
                }
                //Exclusion condition Apply
                var rejectioncond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Rejection")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();

                bool mdf_flag = false;

                if (rejectioncond.Count > 0 && rejectioncond[0].condition_text != "" && mdf_flag == false)
                {
                    string modifiedCondition = "";

                    if (rejectioncond[0].condition_text.Contains("="))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("=", "<>");
                    }
                    else if (rejectioncond[0].condition_text.Contains(">"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace(">", "<");
                    }
                    else if (rejectioncond[0].condition_text.Contains("<"))
                    {
                        modifiedCondition = rejectioncond[0].condition_text.Replace("<", ">");
                    }

                    if (!string.IsNullOrEmpty(modifiedCondition))
                    {
                        query1 = query1 + " AND (" + modifiedCondition + ")";
                        mdf_flag = true;
                    }
                }

                string excelConnectionString = "";

                if (fileExtension == ".xls")
                {
                    excelConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + rawFilePath + ";Extended Properties=\"Excel 8.0;HDR=Yes;IMEX=1\"";
                }
                else if (fileExtension == ".xlsx")
                {
                    excelConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + rawFilePath + ";Extended Properties=\"Excel 12.0;HDR=Yes;IMEX=1\"";
                }
                OleDbConnection excelConnection = new OleDbConnection(excelConnectionString);//connection for excel.

                excelConnection.Open();//excel connection open.
                DataTable dtexcel = new DataTable();//datatable object.
                dtexcel = excelConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                if (dtexcel == null)
                {
                    return null;
                }
                excelConnection.Dispose();

                String[] excelSheets = new String[dtexcel.Rows.Count];
                int t = 0;
                //excel data saves in temp file here.
                foreach (DataRow row in dtexcel.Rows)
                {
                    excelSheets[t] = row["TABLE_NAME"].ToString();
                    t++;
                }
                OleDbConnection excelConnection1 = new OleDbConnection(excelConnectionString);//connection for excel.
                int excel = excelSheets.Count();
                //query = string.Format("Select * from [{0}]",dtexcel.Rows[0]["TABLE_NAME"].ToString() );
                query = string.Format("Select * from [{0}]", "Sheet1$");
                query = query + " Where 1 = true " + query1;

                OleDbCommand thecmd = new OleDbCommand(query, excelConnection1);
                thecmd.CommandTimeout = 0;

                OleDbDataAdapter thedataadapter = new OleDbDataAdapter(thecmd);

                thedataadapter.Fill(ds);

                excelConnection1.Close();

                string directory = System.IO.Path.GetDirectoryName(rawFilePath); // Extract directory path

                if (directory == v_filepath.TrimEnd('\\'))
                {
                    MovetheProcessedfile(rawFilePath, "D:\\Mohan\\CompletedFiles", sched_gid.ToString());
                }
                return ds;
            }
            catch (Exception ex)
            {
                UpdateScheduler(sched_gid, "Failed", "System");
                return ds;
            }
        }

        public void MovetheProcessedfile(string sourceFilePath, string destinationFolderPath, string scheduler_id)
        {
            try
            {

                // Check if the source file exists
                if (System.IO.File.Exists(sourceFilePath))
                {
                    // Check if the destination folder exists, if not create it
                    if (!Directory.Exists(destinationFolderPath))
                    {
                        Directory.CreateDirectory(destinationFolderPath);
                    }

                    // Get the filename from the source file path
                    string fileExtenstion = System.IO.Path.GetExtension(sourceFilePath);
                    string fileName = scheduler_id + fileExtenstion;

                    // Construct the destination file path
                    string destinationFilePath = System.IO.Path.Combine(destinationFolderPath, fileName);

                    // Move the file to the destination folder
                    System.IO.File.Move(sourceFilePath, destinationFilePath);

                    Console.WriteLine("File moved successfully.");
                }
                else
                {
                    Console.WriteLine("Source file does not exist.");
                    //Trace Error
                    var errorLog = new ErrorLog
                    {
                        in_errorlog_pipeline_code = ppl_code,
                        in_errorlog_scheduler_gid = sched_gid,
                        in_errorlog_type = "Catch - Method Name : MovetheProcessedfile",
                        in_errorlog_exception = "Source file does not exist.",
                        in_created_by = initiated_by
                    };
                    Errorlog(errorLog).Wait();
                    UpdateScheduler(sched_gid, "Failed", "System").Wait();
                    Reschedulefornexttime(ppl_code).Wait();
                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : MovetheProcessedfile",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog).Wait();
                UpdateScheduler(sched_gid, "Failed", "System").Wait();
                Reschedulefornexttime(ppl_code).Wait();
            }
        }

        public string DatatableToCSV(DataTable dt, string pipelinecode)
        {
            string destinationTableName = "con_trn_tbcp";

            // Create a MySqlConnection using the connection string
            using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
            {
                var pplcode = pipelinecode;
                csvfilePath = csvfilePath + sched_gid + ".csv";
                // Create the directory if it doesn't exist
                string directory = System.IO.Path.GetDirectoryName(csvfilePath);
                // Create the folder if it doesn't exist.
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Check if the file exists, if not, create the file
                if (!System.IO.File.Exists(csvfilePath))
                {
                    using (FileStream fs = System.IO.File.Create(csvfilePath))
                    {
                        
                    }
                    // Create a StreamWriter to write the CSV file
                    using (StreamWriter writer = new StreamWriter(csvfilePath))
                    {
                        // Write the column headers
                        foreach (DataColumn column in dt.Columns)
                        {
                            writer.Write(column.ColumnName);
                            if (column.Ordinal < dt.Columns.Count - 1)
                            {
                                writer.Write("`~*`");
                            }
                        }
                        writer.WriteLine();

                        // Write the data rows
                        foreach (DataRow row in dt.Rows)
                        {
                            for (int i = 0; i < dt.Columns.Count; i++)
                            {
                                writer.Write(row[i].ToString().Trim().Replace("\n", "").Replace("\r", " "));
                                if (i < dt.Columns.Count - 1)
                                {
                                    writer.Write("`~*`");
                                }
                            }
                            writer.WriteLine();
                        }
                        writer.Close();
                    }
                }
                

                if (connect.State != ConnectionState.Open)
                    connect.Open();

                var shdgid = dbContext.con_trn_tscheduler
                      .Where(p => p.pipeline_code == pipelinecode
                              //&& p.scheduler_status == "Scheduled")
                              && p.scheduler_status == "Scheduled" || p.scheduler_status == "Locked")
                      .Select(a => new
                      {
                          scheduler_gid = a.scheduler_gid
                      })
                      .ToList();
                connect.Close();

                try
                {

                    if (shdgid.Count > 0)
                    {
                        sched_gid = shdgid[0].scheduler_gid;
                        UpdateScheduler(sched_gid, "Initiated", "System");

                        connect.Open();

                        var bulkLoader = new MySqlBulkLoader(connect)
                        {
                            Expressions =  {
                                    "scheduler_gid =" + sched_gid,
                               },
                            TableName = destinationTableName, // Replace with your target table name
                            FieldTerminator = "`~*`",         // CSV field delimiter
                            LineTerminator = lineterm,         // CSV line terminator
                            FileName = csvfilePath,
                            NumberOfLinesToSkip = 1,      // Skip the header row if necessary
                            CharacterSet = "utf8",   // Set the character set
                            Local = true,
                            Timeout = 0
                        };

                        List<string> bcpcolumn = new List<string>();

                        for (int i = 1; dt.Columns.Count >= i; i++)
                        {
                            bcpcolumn.Add("col" + i);

                        }
                        //bcpcolumn.Add("dataset_import_gid");

                        bulkLoader.Columns.AddRange(bcpcolumn);

                        int rowsAffected = bulkLoader.Load();

                        // Delete the csv file after processing
                        if (System.IO.File.Exists(csvfilePath))
                        {
                            System.IO.File.Delete(csvfilePath);
                            Console.WriteLine("File deleted after processing.");
                        }

                        MySqlCommand command = connect.CreateCommand();

                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = "pr_con_set_dataprocessing";
                        command.CommandTimeout = 0;
                        command.Parameters.AddWithValue("pipelinecode", pipelinecode);
                        command.Parameters.AddWithValue("schedulerid", sched_gid);

                        //MySqlParameter out_msg = new MySqlParameter("@out_msg", MySqlDbType.VarChar, 255)
                        //{
                        //    Direction = ParameterDirection.Output
                        //};
                        //MySqlParameter out_result = new MySqlParameter("@out_result", MySqlDbType.Int32)
                        //{
                        //    Direction = ParameterDirection.Output
                        //};

                        //command.Parameters.Add(out_msg);
                        //command.Parameters.Add(out_result);

                        command.ExecuteNonQuery();

                        //string outMsgValue = command.Parameters["@out_msg"].Value.ToString();

                        connect.Close();

                        //UpdateScheduler(sched_gid, "Completed", "System");

                        return (sched_gid.ToString());

                    }
                    else
                    {
                        return ("This Pipeline is not scheduled..!");
                    }
                }

                catch (Exception ex)
                {
                    //Trace Error
                    var errorLog = new ErrorLog
                    {
                        in_errorlog_pipeline_code = ppl_code,
                        in_errorlog_scheduler_gid = sched_gid,
                        in_errorlog_type = "Catch - Method Name : DatatableToCSV",
                        in_errorlog_exception = ex.Message,
                        in_created_by = initiated_by
                    };
                    Errorlog(errorLog);

                    UpdateScheduler(sched_gid, "Failed", "System");

                    return ex.Message.ToString();

                }

            };
        }

        #endregion

        #region First Scheduler
        //First Scheduler
        [HttpGet]
        public IActionResult GetpplScheduledFinList()
        {

            var getsch = dbContext.con_trn_tpplfinalization
                    .Where(a => a.run_type == "Scheduled Run"
                            && a.delete_flag == "N")
                    .Select(a => new
                    {
                        finalization_gid = a.finalization_gid,
                        pipeline_code = a.pipeline_code,
                        run_type = a.run_type,
                        cron_expression = a.cron_expression,
                        extract_mode = a.extract_mode,
                        upload_mode = a.upload_mode,
                        key_field = a.key_field,
                        extract_condition = a.extract_condition,
                        pull_days = a.pull_days,
                        reject_duplicate_flag = a.reject_duplicate_flag,
                        error_mode = a.error_mode
                    })
                    .ToList();

            return Ok(getsch);

        }

        [HttpPost]
        public async Task<IActionResult> CreateScheduler([FromBody] NewSchedulerForothers objsched)
        {
            string msg = "Success";
            try
            {
                var count = await dbContext.con_mst_tpipeline
                        .Where(a => a.pipeline_code == objsched.pipeline_code && a.delete_flag == "N" && a.pipeline_status == "Active")
                        .CountAsync();

                var getsch = dbContext.con_trn_tscheduler
                        .Where(p => p.pipeline_code == objsched.pipeline_code && p.delete_flag == "N"
                        && p.scheduler_status != "Failed" && p.scheduler_status != "Completed")
                        .Select(p => new Scheduler
                        {
                            scheduler_status = p.scheduler_status,
                        })
                        .SingleOrDefault();

                if (count > 0)
                {

                    if (getsch == null)
                    {
                        var sch = new Scheduler()
                        {
                            scheduler_gid = 0,
                            scheduled_date = DateTime.Now,
                            pipeline_code = objsched.pipeline_code,
                            file_name = src_filename,
                            scheduler_start_date = DateTime.Now,
                            scheduler_status = "Scheduled",
                            scheduler_initiated_by = objsched.initiated_by,
                            delete_flag = "N"
                        };

                        await dbContext.con_trn_tscheduler.AddAsync(sch);
                        await dbContext.SaveChangesAsync();

                    }
                    else
                    {
                        msg = "This Pipeline is already in <" + getsch.scheduler_status + "> status";
                    }
                }
                else
                {
                    msg = "This is not a Active pipeline";

                }
            }
            catch (Exception ex)
            {
                msg = ex.Message;
            }

            return Ok(msg);
        }
        #endregion

        #region Second Scheduler

        //Second Scheduler
        [HttpGet]
        public IActionResult GetSchedulerList()
        {
            try
            {
                // Get the current date and time
                DateTime currentDateTime = DateTime.Now;

                // Query records within the less than current date ahead
                var getsch = dbContext.con_trn_tscheduler
                    .Where(p => p.scheduler_status == "Scheduled"
                            && p.pipeline_code != ""
                            && p.scheduler_start_date <= currentDateTime
                            //&& p.file_path == null 
                            //&& p.file_name == null
                            && p.delete_flag == "N")
                    .Select(c => new
                    {
                        scheduler_gid = c.scheduler_gid,
                        scheduled_date = c.scheduled_date,
                        pipeline_code = c.pipeline_code,
                        //file_path = c.file_path.IsNullOrEmpty() ? "" : c.file_path,
                        //file_name = c.file_name.IsNullOrEmpty() ? "" : c.file_name,
                        scheduler_start_date = c.scheduler_start_date,
                        scheduler_end_date = c.scheduler_end_date,
                        scheduler_status = c.scheduler_status,
                        scheduler_initiated_by = c.scheduler_initiated_by,
                        delete_flag = c.delete_flag
                    }).OrderBy(a => a.scheduler_gid)
                    .FirstOrDefault();
                //.ToList();

                return Ok(getsch);

            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        //TGDS_Taskscheduler
        #endregion

        #region Scheduler table insert for Excel


        [HttpPost]
        //public async Task<IActionResult> NewScheduler(string pipeline_code, IFormFile file, string initiated_by)
        public async Task<JsonResult> NewScheduler(NewScheduler objsched)
        {

            //NewScheduler objsched = new NewScheduler();
            //objsched.pipeline_code = pipeline_code;
            //objsched.file = file;
            //objsched.initiated_by = initiated_by;

            string msg = "";
            int out_result = 0;
            src_filename = objsched.file.FileName;
            string[] lastIndex = src_filename.Split(".");
            string fileExtension = lastIndex[1];
            initiated_by = objsched.initiated_by;
            ppl_code = objsched.pipeline_code;
            try
            {
                var count = await dbContext.con_mst_tpipeline
                        .Where(a => a.pipeline_code == objsched.pipeline_code && a.delete_flag == "N" && a.pipeline_status == "Active")
                        .CountAsync();

                var getsch = dbContext.con_trn_tscheduler
                        .Where(p => p.pipeline_code == objsched.pipeline_code && p.delete_flag == "N"
                        && p.scheduler_status != "Failed" && p.scheduler_status != "Completed")
                        .Select(p => new Scheduler
                        {
                            scheduler_status = p.scheduler_status,
                        })
                        .SingleOrDefault();

                if (count > 0)
                {

                    if (getsch == null)
                    {
                        var sch = new Scheduler()
                        {
                            scheduler_gid = 0,//Guid.NewGuid(),
                            scheduled_date = DateTime.Now,
                            pipeline_code = objsched.pipeline_code,
                            //file_path = src_filepath,
                            file_name = src_filename,
                            scheduler_start_date = DateTime.Now,
                            scheduler_status = "Scheduled",
                            scheduler_initiated_by = objsched.initiated_by,
                            delete_flag = "N"
                        };

                        await dbContext.con_trn_tscheduler.AddAsync(sch);
                        await dbContext.SaveChangesAsync();
                        //msg = "Scheduled Successfully...";
                        var lastInsertedId = sch.scheduler_gid;

                        //Update file path and file name 
                        src_filename = lastInsertedId + "." + fileExtension;
                        var src_filepath = uploadfilepath + src_filename;

                        var shdlr = await dbContext.con_trn_tscheduler.FindAsync(lastInsertedId);
                        shdlr.file_path = src_filepath;
                        //shdlr.file_name = src_filename;
                        shdlr.last_update_date = DateTime.Now;

                        await dbContext.SaveChangesAsync();

                        RawfileUploaded(objsched.file);

                        msg = await Exceldatapush(lastInsertedId);
                        out_result = 1;
                    }
                    else
                    {
                        msg = "This Pipeline is already in (" + getsch.scheduler_status + ") status";
                    }
                }
                else
                {
                    msg = "This is not a Active pipeline";

                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : NewScheduler",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog);
                UpdateScheduler(sched_gid, "Failed", "System");
                msg = ex.Message;
            }

            return new JsonResult(new { message = msg, result = out_result });
        }

        public string RawfileUploaded(IFormFile file)
        {
            string msg = "";
            try
            {
                if (file != null && file.Length > 0)
                {
                    if (!Directory.Exists(uploadfilepath))
                    {
                        Directory.CreateDirectory(uploadfilepath);
                    }

                    //var src_filename = file.FileName;
                    var targetFilePath = System.IO.Path.Combine(uploadfilepath, src_filename);
                    using (var stream = new FileStream(targetFilePath, FileMode.Create))
                    {
                        file.CopyTo(stream);
                    }

                    msg = "File uploaded and saved successfully...";
                }
                else
                {
                    //Trace Error
                    var errorLog = new ErrorLog
                    {
                        in_errorlog_pipeline_code = ppl_code,
                        in_errorlog_scheduler_gid = sched_gid,
                        in_errorlog_type = "Catch - Method Name : RawfileUploaded",
                        in_errorlog_exception = "No file was uploaded.",
                        in_created_by = initiated_by
                    };
                    Errorlog(errorLog);
                    UpdateScheduler(sched_gid, "Failed", "System");
                    msg = "No file was uploaded.";
                }
            }
            catch (Exception ex)
            {
                //Trace Error
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = ppl_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : RawfileUploaded",
                    in_errorlog_exception = ex.Message,
                    in_created_by = initiated_by
                };
                Errorlog(errorLog);
                UpdateScheduler(sched_gid, "Failed", "System");
                msg = "An error occurred: " + ex.Message;
            }

            return msg;
        }

        [HttpPost]
        public async Task<IActionResult> UpdateScheduler(int scheduler_gid, string scheduler_status, string initiated_by)
        {
            try
            {
                var shdlr = await dbContext.con_trn_tscheduler.FindAsync(scheduler_gid);

                if (shdlr != null)
                {
                    shdlr.scheduler_status = scheduler_status;
                    shdlr.last_update_date = DateTime.Now;
                    shdlr.scheduler_initiated_by = initiated_by;

                    await dbContext.SaveChangesAsync();

                    return Ok("Record Updated Successfully");
                }
                else
                {
                    return NotFound("Record Not Found for Update");
                }
            }
            catch (DbUpdateConcurrencyException ex)
            {
                // Handle concurrency conflicts
                // You may want to reload the entity and apply changes again or inform the user about the conflict.
                return Conflict($"Concurrency Conflict: {ex.Message}");
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"An error occurred while updating the record: {ex.Message}");
            }
        }

        #endregion

        #region Scheduler table insert for Other Source

        [HttpPost]
        public async Task<IActionResult> TGDS_Taskscheduler([FromBody] NewSchedulerForothers objsched)
        {

            string msg = "";
            try
            {
                var count = await dbContext.con_mst_tpipeline
                        .Where(a => a.pipeline_code == objsched.pipeline_code && a.delete_flag == "N" && a.pipeline_status == "Active")
                        .CountAsync();


                var getsch = dbContext.con_trn_tscheduler
                        .Where(p => p.scheduler_gid == objsched.scheduler_gid
                        && p.pipeline_code == objsched.pipeline_code
                        && p.scheduler_status == "Locked" && p.delete_flag == "N")
                        .Select(p => new Scheduler
                        {
                            scheduler_status = p.scheduler_status,
                            file_path = p.file_path,
                            file_name = p.file_name
                        })
                        .SingleOrDefault();

                if (count > 0)
                {
                    if (getsch != null)
                    {
                        var pipelineWithConnector = await dbContext.con_mst_tpipeline
                       .Where(p => p.pipeline_code == objsched.pipeline_code && p.delete_flag == "N")
                       .Join(
                           dbContext.con_mst_tconnection,
                                       pipeline => pipeline.connection_code,
                           connector => connector.connection_code,
                           (pipeline, connector) => new { Pipeline = pipeline, Connector = connector }
                       )
                       .FirstOrDefaultAsync();

                        if (pipelineWithConnector.Connector.source_db_type == "Excel")
                        {
                            msg = await Exceldatapush(objsched.scheduler_gid);
                        }
                        else
                        {
                            msg = OtherSrcdatapush(objsched.scheduler_gid);
                        }
                    }
                    else
                    {
                        msg = "This Pipeline is already in <" + getsch.scheduler_status + "> status";
                    }
                }
                else
                {
                    msg = "This is not a Active pipeline";

                }
            }
            catch (Exception ex)
            {
                UpdateScheduler(sched_gid, "Failed", "System");
                msg = ex.Message;
            }

            return Ok(msg);
        }

        [HttpPost]
        //public async Task<IActionResult> NewSchedulerForOthers(NewSchedulerForothers objsched)
        public async Task<IActionResult> NewSchedulerForOthers(string pipeline_code, string initiated_by)
        {
            NewScheduler objsched = new NewScheduler();
            objsched.pipeline_code = pipeline_code;
            objsched.initiated_by = initiated_by;

            string msg = "";
            try
            {
                var count = await dbContext.con_mst_tpipeline
                        .Where(a => a.pipeline_code == objsched.pipeline_code && a.delete_flag == "N" && a.pipeline_status == "Active")
                        .CountAsync();

                var getsch = dbContext.con_trn_tscheduler
                        .Where(p => p.pipeline_code == objsched.pipeline_code && p.delete_flag == "N"
                        && p.scheduler_status != "Failed" && p.scheduler_status != "Completed")
                        .Select(p => new Scheduler
                        {
                            scheduler_status = p.scheduler_status,
                        })
                        .SingleOrDefault();

                if (count > 0)
                {

                    if (getsch == null)
                    {
                        var sch = new Scheduler()
                        {
                            scheduler_gid = 0,//Guid.NewGuid(),
                            scheduled_date = DateTime.Now,
                            pipeline_code = objsched.pipeline_code,
                            scheduler_start_date = DateTime.Now,
                            scheduler_status = "Scheduled",
                            scheduler_initiated_by = objsched.initiated_by,
                            delete_flag = "N"
                        };

                        await dbContext.con_trn_tscheduler.AddAsync(sch);
                        await dbContext.SaveChangesAsync();
                        var lastInsertedId = sch.scheduler_gid;

                        await dbContext.SaveChangesAsync();

                        msg = OtherSrcdatapush(lastInsertedId);

                    }
                    else
                    {
                        msg = "This Pipeline is already in <" + getsch.scheduler_status + "> status";
                    }
                }
                else
                {
                    msg = "This is not a Active pipeline";

                }
            }
            catch (Exception ex)
            {
                UpdateScheduler(sched_gid, "Failed", "System");
                msg = ex.Message;
            }

            return Ok(msg);
        }

        public string OtherSrcdatapush(int scheduler_gid)
        {
            string msg = "";
            DataSet dataSet = null;
            sched_gid = scheduler_gid;

            try
            {
                //Get Pipeline codeagainst scheduler id
                var schldpplcode = dbContext.con_trn_tscheduler
                          .Where(a => a.scheduler_gid == scheduler_gid
                          //&& a.scheduler_status == "Scheduled"
                          && a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked"
                          && a.delete_flag == "N")
                          .Select(a => new
                          {
                              scheduler_gid = a.scheduler_gid,
                              pipeline_code = a.pipeline_code,
                              Rawfilepath = a.file_path
                          }).OrderByDescending(a => a.scheduler_gid)
                          .FirstOrDefault();

                if (schldpplcode != null)
                {

                    // Call the FieldmappingDT method
                    DataTable dataTable = FieldmappingDT(schldpplcode.pipeline_code).Result;
                    if (dataTable.Rows.Count <= 0)
                    {
                        UpdateScheduler(scheduler_gid, "Failed", "System");
                        return "Fieldmapping is not done for this pipeline...";
                    }
                    //Get dataset 
                    dataSet = OtherSrcToDataSet(schldpplcode.pipeline_code);

                    int dtrow_count = dataTable.Rows.Count;
                    //int matched_count = 0;

                    //Only for Excel Connector CMD BY Mohan 29-06-2024
                    //for (int m = 0; dataSet.Tables[0].Columns.Count > m; m++)
                    //{
                    //    // DataRow row = dataTable.Rows[m];
                    //    var val = dataTable.Rows[m]["ppl_field_name"].ToString();//row[m].ToString();
                    //    var val1 = dataSet.Tables[0].Columns[m].ToString();
                    //    if (val != val1)
                    //    {
                    //        UpdateScheduler(scheduler_gid, "Failed", "System");
                    //        return "File Header Name Missmatch";
                    //    }

                    //}
                    //CMD START
                    //Expression column name added part cmd by mohan on 06-07-2024
                    //var expressionNames = dbContext.con_trn_tpplsourcefield
                    // .Where(p => p.pipeline_code == schldpplcode.pipeline_code
                    //         && (p.source_type == "Expression")
                    //         && p.delete_flag == "N")
                    // .Select(a => new
                    // {
                    //     sourcefield_name = a.sourcefield_name,
                    //     sourcefield_expression = a.sourcefield_expression
                    // })
                    // .ToList();

                    //for (int i = 0; i < expressionNames.Count; i++)
                    //{
                    //    dataSet.Tables[0].Columns.Add(expressionNames[i].sourcefield_name);
                    //}
                    //CMD END
                    string pplcode = schldpplcode.pipeline_code;

                    msg = DatatableToCSV(dataSet.Tables[0], pplcode);
                }
                else
                {

                    msg = "This Pipeline is not scheduled..!";
                }
            }
            catch (Exception ex)
            {
                UpdateScheduler(scheduler_gid, "Failed", "System");
                return "Error: " + ex.Message;
            }

            return msg;
        }

        [HttpPost]
        public DataSet OtherSrcToDataSet(string pipelinecode)
        {
            string fileExtension = "";
            string query = "select ";

            string result = "";

            DataSet ds = new DataSet();
            try
            {
                var query1 = "";

                var bcpcolumns = from a in dbContext.con_trn_tpplsourcefield
                                 where a.pipeline_code == pipelinecode
                                 where a.sourcefieldmapping_flag == "Y"
                                 //where a.source_type != "Expression"
                                 where a.delete_flag == "N"
                                 orderby a.dataset_table_field_sno
                                 select a.source_type != "Expression" ? a.sourcefield_name : "''";
                var resultList = bcpcolumns.ToList();

                var concatenatedResult = string.Join(",", resultList);

                query = query + concatenatedResult + " from ";

                var ppl = dbContext.con_mst_tpipeline
              .Where(p => p.pipeline_code == pipelinecode && p.pipeline_status == "Active" && p.delete_flag == "N")
              .Select(p => new Pipeline
              {
                  connection_code = p.connection_code,
                  table_view_query_desc = p.table_view_query_desc,
                  db_name = p.db_name
              })
              .SingleOrDefault();

                // Inclusion condition apply
                var filtercond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Filter")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();

                if (filtercond.Count > 0)
                {

                    if (filtercond[0].condition_text.Trim() != "")
                    {
                        var flcond = filtercond[0].condition_text.Replace("[", "`").Replace("]", "`");
                        query1 = " and (" + flcond + ")";
                    }
                }

                //Exclusion condition Apply
                var rejectioncond = dbContext.con_trn_tpplcondition
                 .Where(p => p.pipeline_code == pipelinecode
                         && (p.condition_type == "Rejection")
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     condition_text = a.condition_text
                 })
                 .ToList();


                if (rejectioncond.Count > 0)
                {
                    if (rejectioncond[0].condition_text.Trim() != "")
                    {
                        var rjcond = rejectioncond[0].condition_text.Replace("[", "`").Replace("]", "`");
                        query1 = query1 + " and (" + rjcond + ")";
                    }
                }

                //Extract condition Apply
                var extcond = dbContext.con_trn_tpplfinalization
                 .Where(p => p.pipeline_code == pipelinecode
                         && ((p.extract_condition != "") || (p.extract_condition != null))
                         && p.delete_flag == "N")
                 .Select(a => new
                 {
                     extract_condition = a.extract_condition,
                     last_incremental_val = a.last_incremental_val,
                     extract_mode = a.extract_mode,
                     pull_days = a.pull_days
                 })
                 .ToList();

                var incrementalcond = dbContext.con_trn_tincrementalrecord
                 .Where(p => p.pipeline_code == pipelinecode
                 && p.incremental_field.Contains(".val")
                 && p.delete_flag == "N")
                 .Select(a => new
                 {
                     incremental_field = a.incremental_field,
                     incremental_value = a.incremental_value ?? "1900-01-01" // Default date if null
                 })
                 .ToList();


                if (extcond.Count > 0)
                {

                    string originalString = extcond[0].extract_condition.Trim();

                    if (originalString != "" && extcond[0].extract_mode == "Incremental records")
                    {
                        // Iterate over incrementalcond
                        foreach (var item in incrementalcond)
                        {
                            // Check if the incremental_field exists in the original string
                            if (originalString.Contains(item.incremental_field))
                            {
                                // Replace the incremental_field with the incremental_value
                                originalString = originalString.Replace(item.incremental_field, "'" + item.incremental_value.ToString() + "'");
                            }
                        }
                        if (incrementalcond.Count > 0)
                        {
                            originalString = originalString.Replace("[", "").Replace("]", "");
                            query1 = query1 + " and (" + originalString + ")";
                        }

                    }
                    else if (originalString != "" && extcond[0].extract_mode == "Pull last X days")
                    {
                        originalString = originalString + " > DATE_SUB(CURDATE(), INTERVAL " + extcond[0].pull_days + " DAY)";
                        originalString = originalString.Replace("[", "").Replace("]", "");
                        query1 = query1 + " and (" + originalString + ")";
                    }
                }

                //if (extcond[0].extract_condition.Trim() != "")
                //{
                //    var excond = extcond[0].extract_condition.Replace("[", "`").Replace("]", "`");
                //    excond = excond.Replace("*datefield",
                //             string.IsNullOrEmpty(extcond[0].last_incremental_val) ? "'2000-01-01'" : "'" + extcond[0].last_incremental_val.ToString() + "'").Replace("*integerfield", string.IsNullOrEmpty(extcond[0].last_incremental_val) ? "'0'" : extcond[0].last_incremental_val);
                //    query1 = query1 + " and (" + excond + ")";
                //}


                var connector = dbContext.con_mst_tconnection
              .Where(p => p.connection_code == ppl.connection_code && p.delete_flag == "N")
              .Select(p => new ConnectionModel
              {
                  source_host_name = p.source_host_name,
                  source_port = p.source_port,
                  source_db_user = p.source_db_user,
                  source_db_pwd = p.source_db_pwd,
              })
              .SingleOrDefault();

                // Construct the connection string
                //var src_connstring = $"server={connector.source_host_name}; uid={connector.source_db_user}; pwd={connector.source_db_pwd}; database={ppl.db_name};";

                //DataTable dataTable = new DataTable();

                //string excludeColumns = Getconfigvalue("exclude_column");//"dataset_gid,scheduler_gid,delete_flag";
                //string columnQuery = $@"
                //SELECT GROUP_CONCAT(COLUMN_NAME)
                //FROM INFORMATION_SCHEMA.COLUMNS
                //WHERE TABLE_SCHEMA = '{ppl.db_name}' AND TABLE_NAME = '{ppl.table_view_query_desc}'
                //AND COLUMN_NAME NOT IN ({string.Join(",", excludeColumns.Split(',').Select(c => $"'{c}'"))})";

                //string columns = "";
                //using (MySqlConnection connection = new MySqlConnection(src_connstring))
                //{
                //    connection.Open();

                //    using (MySqlCommand command = new MySqlCommand(columnQuery, connection))
                //    {
                //        columns = (string)command.ExecuteScalar();
                //    }
                //}

                //if (!string.IsNullOrEmpty(columns))
                //{
                //    query = $"SELECT {columns} FROM {ppl.table_view_query_desc} WHERE 1=1 {query1}";

                //    using (MySqlConnection connection = new MySqlConnection(src_connstring))
                //    {
                //        connection.Open();

                //        using (MySqlCommand command = new MySqlCommand(query, connection))
                //        {
                //            using (MySqlDataAdapter adapter = new MySqlDataAdapter(command))
                //            {
                //                adapter.Fill(dataTable);
                //            }
                //        }
                //    }
                //    ds.Tables.Add(dataTable);
                //}

                //return ds;

                var src_connstring = "server=" + connector.source_host_name + "; uid=" +
                                  connector.source_db_user + "; pwd=" + connector.source_db_pwd + "; database=" + ppl.db_name + ";";

                DataTable dataTable = new DataTable();

                query = query + ppl.table_view_query_desc + " where 1=1 " + query1;

                using (MySqlConnection connection = new MySqlConnection(src_connstring))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(command))
                        {
                            adapter.Fill(dataTable);
                        }
                    }
                }
                ds.Tables.Add(dataTable);
                return ds;
            }
            catch (Exception ex)
            {
                UpdateScheduler(sched_gid, "Failed", "System");
                return ds;
            }
        }

        #endregion

        #region Pipeline clone
        [HttpPost]
        public async Task<IActionResult> Pipeline_Cloning([FromBody] pipelineclone objpplclone)
        {
            List<PipelinecloneResult> results = new List<PipelinecloneResult>();
            using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
            {
                if (connect.State != ConnectionState.Open)
                    connect.Open();

                MySqlCommand command = connect.CreateCommand();

                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "pr_con_ins_pipelineclone";

                command.Parameters.AddWithValue("in_pipeline_name", objpplclone.in_pipeline_name);
                command.Parameters.AddWithValue("in_pipeline_code", objpplclone.in_pipeline_code);
                command.Parameters.AddWithValue("in_dataset_code", objpplclone.in_dataset_code);
                MySqlParameter out_srcfile_name = new MySqlParameter("@out_srcfile_name", MySqlDbType.VarChar, 64)
                {
                    Direction = ParameterDirection.Output
                };
                MySqlParameter out_dstfile_name = new MySqlParameter("@out_dstfile_name", MySqlDbType.VarChar, 64)
                {
                    Direction = ParameterDirection.Output
                };
                MySqlParameter out_msg = new MySqlParameter("@out_msg", MySqlDbType.VarChar, 255)
                {
                    Direction = ParameterDirection.Output
                };
                MySqlParameter out_result = new MySqlParameter("@out_result", MySqlDbType.Int32)
                {
                    Direction = ParameterDirection.Output
                };
                command.Parameters.Add(out_srcfile_name);
                command.Parameters.Add(out_dstfile_name);
                command.Parameters.Add(out_msg);
                command.Parameters.Add(out_result);
                command.ExecuteNonQuery();

                PipelinecloneResult result = new PipelinecloneResult
                {
                    SrcFileName = out_srcfile_name.Value.ToString(),
                    DstFileName = out_dstfile_name.Value.ToString(),
                    Message = out_msg.Value.ToString(),
                    Result = Convert.ToInt32(out_result.Value)
                };
                connect.Close();
                if (Convert.ToInt32(out_result.Value) == 1)
                {
                    CopyAndRenameFile(out_srcfile_name.Value.ToString(), out_dstfile_name.Value.ToString());
                }
                results.Add(result);
                return Ok(out_msg.Value.ToString());
            };

        }

        [HttpPost]
        public void CopyAndRenameFile(string sourceFileName, string destinationFileName)
        {
            try
            {
                string[] lastIndex = sourceFileName.Split(".");
                string fileExtension = lastIndex[1];

                destinationFileName = clonefilepath + destinationFileName + "." + fileExtension;
                sourceFileName = clonefilepath + sourceFileName;

                // Check if the source file exists
                if (System.IO.File.Exists(sourceFileName))
                {
                    // Copy the file to the destination and overwrite if it already exists
                    System.IO.File.Copy(sourceFileName, destinationFileName, true);
                }
                else
                {
                    Console.WriteLine("Source file does not exist.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task Errorlog1([FromBody] ErrorLog objerrorlog)
        {
            using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
            {
                if (connect.State != ConnectionState.Open)
                    connect.Open();

                MySqlCommand command = connect.CreateCommand();

                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "pr_con_ins_errorlog";

                command.Parameters.AddWithValue("in_errorlog_pipeline_code", objerrorlog.in_errorlog_pipeline_code);
                command.Parameters.AddWithValue("in_errorlog_scheduler_gid", objerrorlog.in_errorlog_scheduler_gid);
                command.Parameters.AddWithValue("in_errorlog_type", objerrorlog.in_errorlog_type);
                command.Parameters.AddWithValue("in_errorlog_exception", objerrorlog.in_errorlog_exception);
                command.Parameters.AddWithValue("in_created_by", objerrorlog.in_created_by);

                command.ExecuteNonQuery();

            };
        }

        [HttpPost]
        public async Task<IActionResult> Errorlog([FromBody] ErrorLog objerrorlog)
        {
            try
            {
                using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
                {
                    await connect.OpenAsync();

                    using (MySqlCommand command = connect.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = "pr_con_ins_errorlog";

                        command.Parameters.AddWithValue("in_errorlog_pipeline_code", objerrorlog.in_errorlog_pipeline_code);
                        command.Parameters.AddWithValue("in_errorlog_scheduler_gid", objerrorlog.in_errorlog_scheduler_gid);
                        command.Parameters.AddWithValue("in_errorlog_type", objerrorlog.in_errorlog_type);
                        command.Parameters.AddWithValue("in_errorlog_exception", objerrorlog.in_errorlog_exception);
                        command.Parameters.AddWithValue("in_created_by", objerrorlog.in_created_by);

                        await command.ExecuteNonQueryAsync();
                    }
                }

                return Ok("Error log inserted successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"An error occurred while logging the error: {ex.Message}");
            }
        }
        #endregion

        #region getvalagainstPipeline

        [HttpPost]

        [HttpGet]
        public async Task<IActionResult> GetIncrementalRecord(string pipelinecode)
        {
            try
            {
                var records = await dbContext.con_trn_tincrementalrecord
                .Where(item => item.delete_flag == "N" && item.pipeline_code == pipelinecode && item.incremental_field.EndsWith(".val"))
                .OrderByDescending(item => item.incremental_gid)
                .ToListAsync();

                var recordsWithSerialNumber = records.Select((item, index) => new
                {
                    serialNumber = index + 1, // Adding 1 to make the serial number start from 1 instead of 0
                    item.incremental_gid,
                    item.pipeline_code,
                    item.incremental_field,
                    item.incremental_value,
                    item.delete_flag
                    // Add other fields as necessary
                }).ToList();

                return Ok(recordsWithSerialNumber);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }


        [HttpPost]
        public async Task<IActionResult> setIncrementalRecord([FromBody] setIncrementalRecord1 objsetIncrementalRecord)
        {
            using var transaction = await dbContext.Database.BeginTransactionAsync();
            try
            {
                List<IncrementalRecordModel> records;
                try
                {
                    records = JsonConvert.DeserializeObject<List<IncrementalRecordModel>>(objsetIncrementalRecord.jsondata);
                }
                catch (Exception ex)
                {
                    return BadRequest($"Error parsing JSON data: {ex.Message}");
                }

                var deleteincrementRecord = await dbContext.con_trn_tincrementalrecord
                .Where(p => p.pipeline_code == objsetIncrementalRecord.pipeline_code && p.delete_flag == "N" && p.incremental_field.EndsWith(".val"))
                .ToListAsync();

                if (deleteincrementRecord.Any())
                {
                    dbContext.con_trn_tincrementalrecord.RemoveRange(deleteincrementRecord);
                    await dbContext.SaveChangesAsync();
                }
                foreach (var data in records)
                {
                    var objIncrRecords = new PipelineIncrementalRecord()
                    {
                        incremental_gid = 0,
                        pipeline_code = data.pipeline_code,
                        incremental_field = data.incremental_field,
                        incremental_value = data.incremental_value,
                        delete_flag = "N"
                    };

                    await dbContext.con_trn_tincrementalrecord.AddAsync(objIncrRecords);
                }

                await dbContext.SaveChangesAsync();
                await transaction.CommitAsync();
                return Ok("Inserted Successfully");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public string Getconfigvalue(string config_name)
        {
            string config_value = "";
            var config = dbContext.con_mst_tconfig
                                     .Where(p => p.config_name == config_name && p.delete_flag == "N")
                                     .Select(a => new
                                     {
                                         a.config_value
                                     }).FirstOrDefault();

            try
            {

                if (config == null)
                {
                    config_value = "";
                    return config_value;
                }
                else
                {
                    config_value = config.config_value;
                }
                return config_value;
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }
        #endregion

    }
}
