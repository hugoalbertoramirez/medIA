using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace serverMedIA
{
    public interface IController
    {
        DataTable GetTermsToSearch();

        // News:
        int? IsNewsInDB(string url);
        int? InsertNewsInDB(string datePublished, string name, string url, string description, int? idCategory);

        int? IsNews_TermToSearchInDB(int idNews, int idTermToSearch);
        int? InsertNews_TermTosearchInDB(int idNews, int idTermToSearch);
        
        // Category:
        int? IsCategoryInDB(string name);
        int? InsertCategoryInDB(string name);

        // About:
        int? IsAboutInDB(string readLink);
        int? InsertAboutInDB(string readLink, string name);

        int? IsNewsAboutInDB(int idNews, int idAbout);
        bool InsertNewsAboutInDB(List<int> idsAbout, int idNews);

        // Publisher:
        int? IsPublisherInDB(string name);
        int? InsertPublisherInDB(string name);

        int? IsPublisherTypeInDB(string name);
        int? InsertPublisherTypeInDB(string name);

        int? IsPublisher_PublisherTypeInDB(int idPublisher, int idPublisherType);
        int? InsertPublisher_PublisherTypeInDB(int idPublisher, int idPublisherType);
        
        bool InsertNewsPublisherInDB(List<int> idsPublisher, int idNews);

        // Mention:
        int? IsMentionInDB(string name);
        int? InsertMentionInDB(string name);
        
        bool InsertNewsMentionInDB(List<int> idsMention, int idNews);

        // Image:
        bool InsertImage(string contentUrl, string thumbnailContentUrl, string thumbnailWidth, string thumbnailHeight, int idNews);
        int? InsertImageInDB(string contentUrl, string thumbnailContentUrl, string thumbnailWidth, string thumbnailHeight, int idNews);

    }
}
