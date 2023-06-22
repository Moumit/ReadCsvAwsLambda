using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ReadCsvAwsLambda
{
    public class Function
    {

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandlerAsync(S3Event s3Event)
        {
            AmazonS3Client s3Client= new AmazonS3Client();
            foreach (var record in s3Event.Records)
            {
                var tempFile = record.S3;

                var request = new GetObjectRequest()
                {
                    BucketName = tempFile.Bucket.Name,
                    Key = tempFile.Object.Key,
                    
                };

                Console.WriteLine("Begin reading");

                using (var res = await s3Client.GetObjectAsync(request))
                {
                    int count = 0;
                    StreamReader sReader = new StreamReader(res.ResponseStream); //Time out here

                    while (!sReader.EndOfStream) {
                        string? line = sReader.ReadLine();
                        Console.WriteLine(count+" : "+ line);
                        count++;
                    }                    
                }

                Console.WriteLine("End reading");

            }
        }
    }
}