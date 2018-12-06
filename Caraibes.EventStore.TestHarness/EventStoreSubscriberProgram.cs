namespace Caraibes.EventStore.TestHarness
{
    using System;
    using System.Threading.Tasks;

    using Caraibes.Domain;
    using Caraibes.Domain.EventSourcing;
    using Caraibes.Infrastructure;
    using Caraibes.Messaging;

    class EventStoreSubscriberProgram
    {
        static void Main(string[] args)
        {

            Task.Run(Execute).Wait();
            Console.ReadLine();
        }

        private static async Task Execute()
        {
            var connectionFactory = new EventStoreConnectionFactory("ConnectTo=tcp://127.0.0.1:1113;");
            var bus = new EventStoreBus(connectionFactory);
            var canUnsubscribe = bus.RegisterHandler(new EventSubscriptionRequest<Event>(OnEventReceived, "$ce-hello", lastProcessedEventStreamPosition: null));

            var eventStore = new EventStore(connectionFactory, null);

            var streamName = StreamNameBuilder.BuildStreamName<Hello>(Guid.NewGuid());
            await eventStore.CreateNewStream(streamName, new Event[] { new HelloEvent(),  });
            
            bool keepContinue = true;
            while (keepContinue)
            {
                switch (Console.ReadKey().Key)
                {
                    case ConsoleKey.RightArrow:

                        var lastExpectedEventVersion = eventStore.GetStreamLastExpectedEventVersion(streamName);
                        await eventStore.AppendEventsToStream( streamName, new[]  { new HelloEvent(),  }, lastExpectedEventVersion);

                        break;

                    case ConsoleKey.LeftArrow:
                        keepContinue = false;
                        break;
                }
            }
        }

        private static void OnEventReceived(Event evt)
        {
            Console.WriteLine($"Event {evt.LinkEventNumber} arrived");
        }
    }
}