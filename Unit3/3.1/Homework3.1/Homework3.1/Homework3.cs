using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace Homework3 {
    class Program {
        static void Main(string[] args) {

            MainAsync().Wait();
            Console.WriteLine("\n Press enter");
            Console.ReadKey();
        }

        private static async Task MainAsync() {

            var client = new MongoClient();
            var db = client.GetDatabase("school");
            var grades = db.GetCollection<Student>("students");

            var filter = Builders<Student>.Filter;
            var update = Builders<Student>.Update;
            grades.FindSync(Builders<Student>.Filter.Exists(x => x._id))
                .ToList()
                .Select(x => new {
                    Id = x._id,
                    HomeworkScore = x.scores.Where(z => z.type == "homework").OrderBy(p => p.score).First().score
                }).AsParallel()
                    .ForAll(d => grades.FindOneAndUpdate<Student>(
                    filter.Eq(gg => gg._id, d.Id),
                    update.PullFilter(zz => zz.scores, Builders<Score>.Filter.Eq(cv => cv.score, d.HomeworkScore))));
        }
    }

    public class Score {
        public string type { get; set; }
        public double score { get; set; }
    }

    public class Student {
        public int _id { get; set; }
        public string name { get; set; }
        public IEnumerable<Score> scores { get; set; }
    }
}
