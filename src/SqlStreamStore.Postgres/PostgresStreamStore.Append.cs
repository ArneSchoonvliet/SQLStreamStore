﻿namespace SqlStreamStore
{
    using System;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            int maxRetries = 2; //TODO too much? too little? configurable?
            Exception exception;

            int retryCount = 0;
            do
            {
                try
                {
                    AppendResult result;
                    var streamIdInfo = new StreamIdInfo(streamId);

                    using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
                    using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                    using(var command = BuildFunctionCommand(
                        _schema.AppendToStream,
                        transaction,
                        Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                        Parameters.StreamIdOriginal(streamIdInfo.PostgresqlStreamId),
                        Parameters.MetadataStreamId(streamIdInfo.MetadataPosgresqlStreamId),
                        Parameters.ExpectedVersion(expectedVersion),
                        Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                        Parameters.NewStreamMessages(messages)))
                    {
                        try
                        {
                            using(var reader = await command
                                .ExecuteReaderAsync(cancellationToken)
                                .ConfigureAwait(false))
                            {
                                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                                result = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                            }

                            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                        {
                            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);

                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.PostgresqlStreamId.IdOriginal, expectedVersion),
                                streamIdInfo.PostgresqlStreamId.IdOriginal,
                                expectedVersion,
                                ex);
                        }
                    }

                    if(_settings.ScavengeAsynchronously)
                    {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        Task.Run(() => TryScavenge(streamIdInfo, cancellationToken), cancellationToken);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    }
                    else
                    {
                        await TryScavenge(streamIdInfo, cancellationToken).ConfigureAwait(false);
                    }

                    return result;
                }
                catch(PostgresException ex) when(ex.IsDeadlock())
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default; // never actually run
        }
    }
}
