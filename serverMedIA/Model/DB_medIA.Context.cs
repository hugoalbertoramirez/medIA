﻿//------------------------------------------------------------------------------
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
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;
    using System.Data.Entity.ModelConfiguration.Conventions;

    public partial class Entities : DbContext
    {
        public Entities()
            : base("name=Entities")
        {
        }
    
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            //throw new UnintentionalCodeFirstException();
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    
        public virtual DbSet<KeyPhrase> KeyPhrases { get; set; }
        public virtual DbSet<Opinion> Opinions { get; set; }
        public virtual DbSet<OpinionLimit> OpinionLimits { get; set; }
        public virtual DbSet<Publisher> Publishers { get; set; }
        public virtual DbSet<Publisher_State> Publisher_State { get; set; }
        public virtual DbSet<PublisherType> PublisherTypes { get; set; }
        public virtual DbSet<State> States { get; set; }
        public virtual DbSet<TermToSearch> TermToSearches { get; set; }
        public virtual DbSet<User_Video> User_Video { get; set; }
        public virtual DbSet<Video_TermToSearch> Video_TermToSearch { get; set; }
        public virtual DbSet<About> Abouts { get; set; }
        public virtual DbSet<Category> Categories { get; set; }
        public virtual DbSet<Favorite_News> Favorite_News { get; set; }
        public virtual DbSet<Image> Images { get; set; }
        public virtual DbSet<Mention> Mentions { get; set; }
        public virtual DbSet<News> News { get; set; }
        public virtual DbSet<News_About> News_About { get; set; }
        public virtual DbSet<News_KeyPhrase> News_KeyPhrase { get; set; }
        public virtual DbSet<News_Mention> News_Mention { get; set; }
        public virtual DbSet<News_Publisher> News_Publisher { get; set; }
        public virtual DbSet<News_TermToSearch> News_TermToSearch { get; set; }
        public virtual DbSet<News_Video> News_Video { get; set; }
        public virtual DbSet<Creator> Creators { get; set; }
        public virtual DbSet<EncodingFormat> EncodingFormats { get; set; }
        public virtual DbSet<Favorite_Video> Favorite_Video { get; set; }
        public virtual DbSet<Video> Videos { get; set; }
        public virtual DbSet<Video_KeyPhrase> Video_KeyPhrase { get; set; }
        public virtual DbSet<Video_Opinion> Video_Opinion { get; set; }
        public virtual DbSet<VideoIndexer> VideoIndexers { get; set; }
    }
}
