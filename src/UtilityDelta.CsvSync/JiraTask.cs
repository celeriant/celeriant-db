using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilityDelta.CsvSync
{
    public class JiraTask
    {
        [Name("Task ID")]
        public string TaskId { get; set; }
        public string Summary { get; set; }
        public string Status { get; set; }
        [Name("Assigned To")]
        public string AssignedTo { get; set; }
        [Name("Date Created")]
        public DateTime DateCreated { get; set; }
        [Name("Date Last Modified")]
        public DateTime DateLastModified { get; set; }
        [Name("Parent Task")]
        public string ParentTask { get; set; }
        public string Dependencies { get; set; }  // You can split this into a List<string> if needed

        public override string ToString()
        {
            return $"{TaskId} | {Status} | {AssignedTo} | Parent: {ParentTask}";
        }
    }
}
