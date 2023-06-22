using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Runtime.Internal.Transform;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using System.Linq;
using System.Text.Json;

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


            var lstHeaders = "HertzUnitArea,HertzUnitID,VIN,Mileage".Split(',');


            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            // This client will access the US East 1 region.
            clientConfig.RegionEndpoint = RegionEndpoint.USEast1;
            AmazonDynamoDBClient dBClient = new AmazonDynamoDBClient(clientConfig);

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
                    //int count = 0;
                    StreamReader sReader = new StreamReader(res.ResponseStream); //Time out here

                    while (!sReader.EndOfStream) {
                        string? line = sReader.ReadLine();
                        //Console.WriteLine(count+" : "+ line);

                        var values = line.Split(',').ToList();
                        var finalRow= new Dictionary<string, AttributeValue>();                      

                        finalRow.Add("NoPrimary", new AttributeValue { S= Guid.NewGuid().ToString() });
                        int countIndex = 0;
                        foreach (var value in lstHeaders)
                        {
                            var valueItem = values.ElementAtOrDefault(countIndex++) ?? string.Empty;
                            finalRow.Add(value, new AttributeValue { S= valueItem });
                        }
                        var createItemRequest = new PutItemRequest
                        {
                            TableName = "DDUS",
                            Item = finalRow
                        };
                        
                        //Console.WriteLine(JsonSerializer.Serialize(finalRow));

                        var response = await dBClient.PutItemAsync(createItemRequest);

                        
                    }                    
                }



                Console.WriteLine("End reading");

            }
        }
    }
}