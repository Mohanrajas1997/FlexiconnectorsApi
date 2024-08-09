using System;
using System.Text.Json.Nodes;
using Microsoft.Graph.Models;

namespace MysqlEfCoreDemo.Models
{
	public class FieldConfigModel
	{
		public Int64? fieldconfig_gid { get; set; } = 0;
		public string fieldconfig_master_code { get; set; } = null;
		public string fieldconfig_expressions { get; set; } = null;
		public string fieldconfig_expression_label { get; set; } = null;
		public JsonArray fieldconfig_dynamicfields { get; set; }
		public char delete_flag { get; set; } = 'N';
	}
}
