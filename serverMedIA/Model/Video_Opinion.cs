//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Model
{
    using System;
    using System.Collections.Generic;
    
    public partial class Video_Opinion
    {
        public int id { get; set; }
        public int idVideo { get; set; }
        public int idOpinion { get; set; }
        public System.TimeSpan startTime { get; set; }
        public System.TimeSpan endTime { get; set; }
        public bool status { get; set; }
    
        public virtual Opinion Opinion { get; set; }
        public virtual Video Video { get; set; }
    }
}