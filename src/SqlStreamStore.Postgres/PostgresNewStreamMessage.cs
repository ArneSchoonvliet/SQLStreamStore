﻿namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Streams;

    internal class PostgresNewStreamMessage
    {
        internal const string DataTypeName = "new_stream_message";

        public Guid MessageId { get; set; }
        public string JsonData { get; set; }
        public string JsonMetadata { get; set; }
        public string Type { get; set; }

        public static PostgresNewStreamMessage FromNewStreamMessage(NewStreamMessage message)
            => new PostgresNewStreamMessage
            {
                MessageId = message.MessageId,
                Type = message.Type,
                JsonData = message.JsonData,
                JsonMetadata = string.IsNullOrEmpty(message.JsonMetadata) ? null : message.JsonMetadata,
            };
    }
}