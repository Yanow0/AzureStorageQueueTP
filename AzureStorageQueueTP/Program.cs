using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration; // Namespace for ConfigurationManager
using System.Threading.Tasks; // Namespace for Task
using Azure.Storage.Queues; // Namespace for Queue storage types
using Azure.Storage.Queues.Models; // Namespace for PeekedMessage
using System.Threading;

namespace AzureStorageQueueTP
{
    class Program
    {
        static void Main(string[] args)
        {
            Program p = new Program();
            Thread t = new Thread(new ThreadStart(InsertMessagesConstantly));
            Thread t2 = new Thread(new ThreadStart(TreatMessages));
            Thread t3 = new Thread(new ThreadStart(TreatMessages));
            CreateQueue("queue");
            t.Start();
            t2.Start();
            t3.Start();

        }

        public static void InsertMessagesConstantly()
        {
            while (true)
            {
                InsertMessage("queue","message");
               
            }
        }

        public static void TreatMessages()
        {
            int fail = 0;
            while (true)
            {
                if (fail != 9)
                {
                    DequeueMessage("queue");
                }
                
                fail++;

                if (fail > 9)
                {
                    fail = 0;
                }
            }
        }

        //-------------------------------------------------
        // Create a message queue
        //-------------------------------------------------
        public static bool CreateQueue(string queueName)
        {
            try
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to create and manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                // Create the queue
                queueClient.CreateIfNotExists();

                if (queueClient.Exists())
                {
                    Console.WriteLine($"Queue created: '{queueClient.Name}'");
                    return true;
                }
                else
                {
                    Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}\n\n");
                Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                return false;
            }
        }

        public static void InsertMessage(string queueName, string message)
        {
            // Get the connection string from app settings
            string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

            // Instantiate a QueueClient which will be used to create and manipulate the queue
            QueueClient queueClient = new QueueClient(connectionString, queueName);

            // Create the queue if it doesn't already exist
            queueClient.CreateIfNotExists();

            if (queueClient.Exists())
            {
                // Send a message to the queue
                queueClient.SendMessage(message);
            }

            Console.WriteLine($"Inserted: {message}");
        }

        //-------------------------------------------------
        // Process and remove a message from the queue
        //-------------------------------------------------
        public static void DequeueMessage(string queueName)
        {
            // Get the connection string from app settings
            string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

            // Instantiate a QueueClient which will be used to manipulate the queue
            QueueClient queueClient = new QueueClient(connectionString, queueName);

            if (queueClient.Exists())
            {
                // Get the next message
                QueueMessage[] retrievedMessage = queueClient.ReceiveMessages();

                if (retrievedMessage.Length != 0)
                {
                    // Process (i.e. print) the message in less than 30 seconds
                    Console.WriteLine($"Dequeued message: '{retrievedMessage[0].Body}'");

                    // Delete the message
                    queueClient.DeleteMessage(retrievedMessage[0].MessageId, retrievedMessage[0].PopReceipt);
                }
            }
        }
    }



}

