﻿namespace Cedar.EventStore.Streams
{
    using System;

    public sealed class StreamEvent
    {
        public readonly long Checkpoint;
        public readonly DateTimeOffset Created;
        public readonly Guid EventId;
        public readonly string JsonData;
        public readonly string JsonMetadata;
        public readonly int StreamVersion;
        public readonly string StreamId;
        public readonly string Type;

        public StreamEvent(
            string streamId,
            Guid eventId,
            int streamVersion,
            long checkpoint,
            DateTimeOffset created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            EventId = eventId;
            StreamId = streamId;
            StreamVersion = streamVersion;
            Checkpoint = checkpoint;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}