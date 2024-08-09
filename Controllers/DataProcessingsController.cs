using System.Threading.Tasks;
using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MysqlEfCoreDemo.Data;
using System.Linq;
using Google.Protobuf.WellKnownTypes;
using MysqlEfCoreDemo.Models;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;
using Castle.Core.Resource;
using Microsoft.Graph.Models;
using System.Xml;
using Microsoft.IdentityModel.Tokens;
using NPOI.SS.Formula.Functions;

namespace MysqlEfCoreDemo.Controllers
{
	public class DataProcessingsController : Controller
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
		string errorlogfilePath = "D:\\Mohan\\error_log.txt";
		string errormsg = "";
		long datasetimportgid = 0;
		#endregion
		public DataProcessingsController(MyDbContext dbContext, IConfiguration configuration)
		{
			_configuration = configuration;
			targetconnectionString = _configuration.GetConnectionString("targetMysql");
			conn = _configuration["conn"];
			trg_hstname = _configuration["trg_hstname"];
			trg_dbname = _configuration["trg_dbname"];
			trg_username = _configuration["trg_username"];
			trg_password = _configuration["trg_password"];
			csvfilePath = _configuration["csvfilePath"];
			this.dbContext = dbContext;

		}

		#region Qcd Master With DataProcessing
		[HttpGet]
		public async Task<IActionResult> GetMasterDatawithCode(string parentCode = "", string dependCode = "")
		{

			try
			{
				if (string.IsNullOrEmpty(parentCode))
				{
					parentCode = "1";

                }
				var query = from entity in dbContext.con_mst_tmaster select entity;
				if (parentCode != "" && dependCode == "")
				{
					query = from entity in dbContext.con_mst_tmaster
							where entity.parent_code == parentCode // Replace with your condition
							where entity.delete_flag == 'N'
							select entity;
				}
				else if (parentCode != "" && dependCode != "")
				{
					query = from entity in dbContext.con_mst_tmaster
							where entity.parent_code == parentCode && entity.depend_code == dependCode // Replace with your condition
							where entity.delete_flag == 'N'
							select entity;
				}


				var result = await query.ToListAsync();
				//var result = await dbContext.con_mst_tmaster.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		[HttpGet]
		public async Task<IActionResult> GetMasterData(Int64 parentgid = 0, Int64 dependgid = 0)
		{

			try
			{
				var query = from entity in dbContext.con_mst_tmaster select entity;
				if (parentgid != 0 && dependgid == 0)
				{
					query = from entity in dbContext.con_mst_tmaster
							where entity.parent_gid == parentgid // Replace with your condition
							where entity.delete_flag == 'N'
							select entity;
				}
				else if (parentgid != 0 && dependgid != 0)
				{
					query = from entity in dbContext.con_mst_tmaster
							where entity.depend_gid == dependgid && entity.parent_gid == parentgid// Replace with your condition
							where entity.delete_flag == 'N'
							select entity;
				}


				var result = await query.ToListAsync();
				//var result = await dbContext.con_mst_tmaster.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		public async Task<IActionResult> GetFieldMapping(String pipelineCode = "")
		{

			try
			{
				var query = from entity in dbContext.con_trn_tpplfieldmapping select new { entity.pplfieldmapping_gid, entity.ppl_field_name };

				if (pipelineCode != "")
				{
					query = from entity in dbContext.con_trn_tpplfieldmapping
							join ds in dbContext.con_mst_tdataset_field
                             on entity.dataset_code equals ds.dataset_code 
                            where entity.pipeline_code == pipelineCode
								&& entity.delete_flag == "N"
							    //&& entity.pplfieldmapping_flag == 1
								&& entity.ppl_field_name != "-- Select --"
                                && ds.delete_flag == "N"
                                && entity.dataset_field_name == ds.dataset_field_name
                            select new { entity.pplfieldmapping_gid, ppl_field_name = entity.ppl_field_name + " - " + ds.dataset_field_desc };
				}

				var result = await query.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		[HttpPost]
		public async Task<IActionResult> setDataprocessing([FromBody] DataProcessingModel dataProcessingModel)
		{

			try
			{
				var query = from entity in dbContext.con_mst_tmaster select entity;
				if (dataProcessingModel.delete_flag == 'N')
				{
					if (dataProcessingModel.dataprocessing_child_master_code != "" && dataProcessingModel.dataprocessing_child_master_code != null)
					{
						string masterCode = dataProcessingModel.dataprocessing_child_master_code;
						string parentCode = dataProcessingModel.dataprocessing_parent_master_code;
                        query = from entity in dbContext.con_mst_tmaster
								where entity.master_code == masterCode // Replace with your condition
								where entity.parent_code == parentCode 
                                where entity.delete_flag == 'N'
								select entity;
						var result = await query.ToListAsync();
						dataProcessingModel.dataprocessing_child_master_gid = result[0].master_gid;
						dataProcessingModel.dataprocessing_parent_master_gid = result[0].parent_gid;
						dataProcessingModel.dataprocessing_master_gid = result[0].depend_gid;
					}
					else if (dataProcessingModel.dataprocessing_child_master_code.IsNullOrEmpty() && dataProcessingModel.dataprocessing_parent_master_code != "")
					{
						string masterCode = dataProcessingModel.dataprocessing_parent_master_code;
						query = from entity in dbContext.con_mst_tmaster
								where entity.master_code == masterCode // Replace with your condition
								where entity.delete_flag == 'N'
								select entity;
						var result = await query.ToListAsync();
						dataProcessingModel.dataprocessing_child_master_gid = 0;
						dataProcessingModel.dataprocessing_parent_master_gid = result[0].master_gid;
						dataProcessingModel.dataprocessing_master_gid = result[0].parent_gid;
					}
					else if (dataProcessingModel.dataprocessing_child_master_code.IsNullOrEmpty() && dataProcessingModel.dataprocessing_parent_master_code.IsNullOrEmpty())
					{
						string masterCode = dataProcessingModel.dataprocessing_master_code;
						query = from entity in dbContext.con_mst_tmaster
								where entity.master_code == masterCode // Replace with your condition
								where entity.delete_flag == 'N'
								select entity;
						var result = await query.ToListAsync();
						dataProcessingModel.dataprocessing_child_master_gid = 0;
						dataProcessingModel.dataprocessing_parent_master_gid = 0;
						dataProcessingModel.dataprocessing_master_gid = result[0].master_gid;
					}
				}

				var dataProcessings = new dataProcessing();
				var Result = "";
				if (dataProcessingModel.delete_flag == 'N')
				{

					if (dataProcessingModel.dataprocessing_gid <= 0)
					{
						dataProcessings = new dataProcessing()
						{
							dataprocessing_gid = dataProcessingModel.dataprocessing_gid,//Guid.NewGuid(),
							dataprocessing_header_gid = dataProcessingModel.dataprocessing_header_gid,
							dataprocessing_child_master_gid = dataProcessingModel.dataprocessing_child_master_gid,
							dataprocessing_parent_master_gid = dataProcessingModel.dataprocessing_parent_master_gid,
							dataprocessing_master_gid = dataProcessingModel.dataprocessing_master_gid,
							dataprocessing_pipeline_gid = dataProcessingModel.dataprocessing_pipeline_gid,
							dataprocessing_pplfieldmapping_gid = dataProcessingModel.dataprocessing_pplfieldmapping_gid,
							dataprocessing_orderby = dataProcessingModel.dataprocessing_orderby,
							dataprocessing_param1 = dataProcessingModel.dataprocessing_param1.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param1,
							dataprocessing_param2 = dataProcessingModel.dataprocessing_param2.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param2,
							dataprocessing_param3 = dataProcessingModel.dataprocessing_param3.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param3,
							created_date = DateTime.Now,
							created_by = "1",
							delete_flag = 'N'
						};
						await dbContext.con_mst_tdataprocessing.AddAsync(dataProcessings);
						Result = "Dataprocessing Successfuly Inserted.";
						//return Ok(Result);
					}
					else
					{
						// Retrieve the entity you want to update (e.g., by its primary key)
						var entityToUpdate = await dbContext.con_mst_tdataprocessing.FindAsync(dataProcessingModel.dataprocessing_gid);

						if (entityToUpdate != null)
						{
							// Update the properties of the entity
							entityToUpdate.dataprocessing_gid = dataProcessingModel.dataprocessing_gid;//Guid.NewGuid(),
							entityToUpdate.dataprocessing_header_gid = dataProcessingModel.dataprocessing_header_gid;
							entityToUpdate.dataprocessing_child_master_gid = dataProcessingModel.dataprocessing_child_master_gid;
							entityToUpdate.dataprocessing_parent_master_gid = dataProcessingModel.dataprocessing_parent_master_gid;
							entityToUpdate.dataprocessing_master_gid = dataProcessingModel.dataprocessing_master_gid;
							entityToUpdate.dataprocessing_pipeline_gid = dataProcessingModel.dataprocessing_pipeline_gid;
							entityToUpdate.dataprocessing_pplfieldmapping_gid = dataProcessingModel.dataprocessing_pplfieldmapping_gid;
							entityToUpdate.dataprocessing_orderby = dataProcessingModel.dataprocessing_orderby;
							entityToUpdate.dataprocessing_param1 = dataProcessingModel.dataprocessing_param1.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param1;
							entityToUpdate.dataprocessing_param2 = dataProcessingModel.dataprocessing_param2.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param2;
							entityToUpdate.dataprocessing_param3 = dataProcessingModel.dataprocessing_param3.IsNullOrEmpty() ? "" : dataProcessingModel.dataprocessing_param3;
							entityToUpdate.updated_date = DateTime.Now;
							entityToUpdate.updated_by = "1";
							entityToUpdate.delete_flag = 'N';
						}
						Result = "Dataprocessing Successfuly Updated.";
					}
				}
				else
				{
					// Retrieve the entity you want to update (e.g., by its primary key)
					var entityToUpdate = await dbContext.con_mst_tdataprocessing.FindAsync(dataProcessingModel.dataprocessing_gid);

					if (entityToUpdate != null)
					{
						// Update the properties of the entity
						entityToUpdate.updated_date = DateTime.Now;
						entityToUpdate.updated_by = "1";
						entityToUpdate.delete_flag = 'Y';
					}
					Result = "Dataprocessing Successfuly Deleted.";
				}

				// await dbContext.con_mst_tdataprocessing.AddAsync(dataProcessings);
				await dbContext.SaveChangesAsync();
				return Ok(Result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		[HttpGet]
		public async Task<IActionResult> GetDataprocessingList(string pipelineCode = "", Int64 headerGid = 0)
		{

			try
			{
				var query = from entity in dbContext.con_mst_tdataprocessing select entity;
				Int64 masterGid = 0;
				if (pipelineCode != "")
				{
					//var query1 = (from entity in dbContext.con_mst_tmaster
					//        where entity.master_code == masterCode // Replace with your condition
					//        where entity.delete_flag == 'N'
					//        select entity);
					//var result1 = await query1.ToListAsync();
					//masterGid = result1[0].master_gid;
					//(IQueryable<dataProcessing>)
					query = (from dataprocessing in dbContext.con_mst_tdataprocessing
							 join fieldmapping in dbContext.con_trn_tpplfieldmapping
							 on dataprocessing.dataprocessing_pplfieldmapping_gid equals fieldmapping.pplfieldmapping_gid
							 join master in dbContext.con_mst_tmaster
							 on dataprocessing.dataprocessing_master_gid equals master.master_gid
							 join parentmaster in dbContext.con_mst_tmaster
							 on dataprocessing.dataprocessing_parent_master_gid equals parentmaster.master_gid
							 join childmaster in dbContext.con_mst_tmaster
							 on dataprocessing.dataprocessing_child_master_gid equals childmaster.master_gid into childmasterGroup
							 from childmaster in childmasterGroup.DefaultIfEmpty()
							 join dataprocessingheader in dbContext.con_mst_tdataprocessingheader
							 on dataprocessing.dataprocessing_header_gid equals dataprocessingheader.dataprocessingheader_gid
							 where dataprocessingheader.dataprocessingheader_pipeline_code == pipelineCode // Replace with your condition
							 where dataprocessingheader.dataprocessingheader_gid == headerGid
							 where dataprocessing.delete_flag == 'N'                                                        // where dataprocessing.dataprocessing_master_gid == masterGid
							 where dataprocessing.delete_flag == 'N'
							 where fieldmapping.delete_flag == "N"
							 where master.delete_flag == 'N'
							 select new dataProcessing
							 {
								 dataprocessing_gid = dataprocessing.dataprocessing_gid,
								 dataprocessing_header_gid = dataprocessing.dataprocessing_header_gid,
								 dataprocessing_pipeline_gid = dataprocessing.dataprocessing_pipeline_gid,
								 dataprocessing_child_master_gid = childmaster != null ? dataprocessing.dataprocessing_child_master_gid : 0,
								 dataprocessing_parent_master_gid = dataprocessing.dataprocessing_parent_master_gid,
								 dataprocessing_master_gid = dataprocessing.dataprocessing_master_gid,
								 dataprocessing_pplfieldmapping_gid = dataprocessing.dataprocessing_pplfieldmapping_gid,
								 dataprocessing_orderby = dataprocessing.dataprocessing_orderby,
								 dataprocessing_param1 = dataprocessing.dataprocessing_param1,
								 dataprocessing_param2 = dataprocessing.dataprocessing_param2,
								 dataprocessing_param3 = dataprocessing.dataprocessing_param3,
								 dataprocessing_ppl_field_name = fieldmapping.ppl_field_name,
								 dataprocessing_master_name = master.master_name,
								 dataprocessing_parent_master_name = parentmaster.master_name,
								 dataprocessing_child_master_name = childmaster != null ? childmaster.master_name : "-",

								 dataprocessing_master_code = master.master_code,
								 dataprocessing_parent_master_code = parentmaster.master_code,
								 dataprocessing_child_master_code = childmaster != null ? childmaster.master_code : "-"
							 });
				}



				var result = await query.ToListAsync();
				//var result = await dbContext.con_mst_tmaster.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		[HttpGet]
		public async Task<IActionResult> GetDataprocessingHeaderList(string pipelineCode = "")
		{
			try
			{
				var query = from entity in dbContext.con_mst_tdataprocessingheader select entity;

				query = (from dataProcessingheader in dbContext.con_mst_tdataprocessingheader
						 where dataProcessingheader.delete_flag == 'N'
						 where dataProcessingheader.dataprocessingheader_pipeline_code == pipelineCode
						 select new dataProcessingheader
						 {
							 dataprocessingheader_gid = dataProcessingheader.dataprocessingheader_gid,
							 dataprocessingheader_pplfieldmapping_gid = dataProcessingheader.dataprocessingheader_pplfieldmapping_gid,
							 dataprocessingheader_pipeline_code = dataProcessingheader.dataprocessingheader_pipeline_code,
							 dataprocessingheader_seqno = dataProcessingheader.dataprocessingheader_seqno,
							 dataprocessingheader_ppl_field_name = dataProcessingheader.dataprocessingheader_ppl_field_name
						 });



				var result = await query.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}
		[HttpPost]
		public async Task<IActionResult> setDataprocessingHeader([FromBody] DataProcessingHeaderModel dataProcessingheaderModel)
		{

			try
			{
				var dataProcessings = new dataProcessingheader();
				var Result = "";
				if (dataProcessingheaderModel.delete_flag == 'N')
				{

					if (dataProcessingheaderModel.dataprocessingheader_gid <= 0)
					{
						dataProcessings = new dataProcessingheader()
						{
							dataprocessingheader_gid = dataProcessingheaderModel.dataprocessingheader_gid,//Guid.NewGuid(),
							dataprocessingheader_pplfieldmapping_gid = dataProcessingheaderModel.dataprocessingheader_pplfieldmapping_gid,
							dataprocessingheader_pipeline_code = dataProcessingheaderModel.dataprocessingheader_pipeline_code,
							dataprocessingheader_seqno = dataProcessingheaderModel.dataprocessingheader_seqno,
							dataprocessingheader_ppl_field_name = dataProcessingheaderModel.dataprocessingheader_ppl_field_name,
							delete_flag = dataProcessingheaderModel.delete_flag,
							created_date = DateTime.Now,
							created_by = "1"
						};
						await dbContext.con_mst_tdataprocessingheader.AddAsync(dataProcessings);
						Result = "ProcessingSourceField Successfuly Inserted.";

					}
					else
					{
						// Retrieve the entity you want to update (e.g., by its primary key)
						var entityToUpdate = await dbContext.con_mst_tdataprocessingheader.FindAsync(dataProcessingheaderModel.dataprocessingheader_gid);

						if (entityToUpdate != null)
						{
							// Update the properties of the entity
							entityToUpdate.dataprocessingheader_gid = dataProcessingheaderModel.dataprocessingheader_gid;//Guid.NewGuid(),
							entityToUpdate.dataprocessingheader_pplfieldmapping_gid = dataProcessingheaderModel.dataprocessingheader_pplfieldmapping_gid;
							entityToUpdate.dataprocessingheader_pipeline_code = dataProcessingheaderModel.dataprocessingheader_pipeline_code;
							entityToUpdate.dataprocessingheader_seqno = dataProcessingheaderModel.dataprocessingheader_seqno;
							entityToUpdate.dataprocessingheader_ppl_field_name = dataProcessingheaderModel.dataprocessingheader_ppl_field_name;
							entityToUpdate.updated_date = DateTime.Now;
							entityToUpdate.updated_by = "1";
							entityToUpdate.delete_flag = 'N';
						}
						Result = "ProcessingSourceField Successfuly Updated.";
					}
				}
				else
				{
					// Retrieve the entity you want to update (e.g., by its primary key)
					var entityToUpdate = await dbContext.con_mst_tdataprocessingheader.FindAsync(dataProcessingheaderModel.dataprocessingheader_gid);

					if (entityToUpdate != null)
					{
						// Update the properties of the entity
						entityToUpdate.updated_date = DateTime.Now;
						entityToUpdate.updated_by = "1";
						entityToUpdate.delete_flag = 'Y';
					}
					Result = "Dataprocessing Successfuly Deleted.";
				}
				await dbContext.SaveChangesAsync();
				return Ok(Result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}

		[HttpGet]
		public async Task<IActionResult> GetfieldConfig()
		{
			try
			{
				var query = from entity in dbContext.con_mst_tfieldconfig select entity;

				query = (from fieldConfig in dbContext.con_mst_tfieldconfig
						 where fieldConfig.delete_flag == 'N'
						 select new fieldConfig
						 {
							 fieldconfig_gid = fieldConfig.fieldconfig_gid,
							 fieldconfig_master_code = fieldConfig.fieldconfig_master_code,
							 fieldconfig_expressions = fieldConfig.fieldconfig_expressions,
							 fieldconfig_expression_label = fieldConfig.fieldconfig_expression_label,
							 fieldconfig_dynamicfields = fieldConfig.fieldconfig_dynamicfields
						 });



				var result = await query.ToListAsync();
				return Ok(result);
			}
			catch (Exception ex)
			{
				return BadRequest($"[Error: {ex.Message}]");
			}
		}
		#endregion
	}
}
