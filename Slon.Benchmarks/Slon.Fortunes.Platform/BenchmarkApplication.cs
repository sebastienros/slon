using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.Extensions.ObjectPool;
using RazorSlices;

namespace Slon.Fortunes.Platform;

public sealed partial class BenchmarkApplication
{
    private static readonly DefaultObjectPool<ChunkedPipeWriter> ChunkedWriterPool =
        new(new ChunkedWriterObjectPolicy());

    private RequestType _requestType;

    internal static FortuneDatabase Database { get; set; } = null!;

    public void OnStartLine(
        HttpVersionAndMethod versionAndMethod,
        TargetOffsetPathLength targetPath,
        Span<byte> startLine)
    {
        _requestType = versionAndMethod.Method ==
            Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http.HttpMethod.Get &&
            startLine.Slice(targetPath.Offset, targetPath.Length).SequenceEqual("/fortunes"u8)
                ? RequestType.Fortunes
                : RequestType.NotRecognized;
    }

    private ValueTask ProcessRequestAsync() => _requestType switch
    {
        RequestType.Fortunes => RenderDatabaseAsync(),
        _ => new(OutputEmptyAsync(Writer)),
    };

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask RenderDatabaseAsync()
    {
        var fortunes = await Database.LoadAsync(ConnectionClosed).ConfigureAwait(false);
        try
        {
            var template = Templates.Fortunes.Create(fortunes);
            await OutputFortunesAsync(Writer, template).ConfigureAwait(false);
        }
        finally
        {
            Database.Return(fortunes);
        }
    }

    private ValueTask OutputFortunesAsync(
        PipeWriter pipeWriter,
        RazorSlice template)
    {
        var chunkedWriter = StartResponse(pipeWriter);
        var renderTask = template.RenderAsync(chunkedWriter, HtmlEncoder);
        if (renderTask.IsCompletedSuccessfully)
        {
            renderTask.GetAwaiter().GetResult();
            EndTemplateRendering(chunkedWriter, template);
            return ValueTask.CompletedTask;
        }

        return AwaitTemplateRenderTask(renderTask, chunkedWriter, template);
    }

    private static Task OutputEmptyAsync(PipeWriter pipeWriter)
    {
        var writer = StartResponse(pipeWriter);
        writer.Complete();
        ReturnChunkedWriter(writer);
        return Task.CompletedTask;
    }

    private static ChunkedPipeWriter StartResponse(PipeWriter pipeWriter)
    {
        var preamble =
            "HTTP/1.1 200 OK\r\nServer: K\r\nContent-Type: text/html; charset=utf-8\r\nTransfer-Encoding: chunked"u8;
        var headersLength = preamble.Length + DateHeader.HeaderBytes.Length;
        Span<byte> headers = stackalloc byte[headersLength];
        preamble.CopyTo(headers);
        DateHeader.HeaderBytes.CopyTo(headers[preamble.Length..]);

        var writer = ChunkedWriterPool.Get();
        writer.SetOutput(pipeWriter, headers, 2048);
        return writer;
    }

    private static async ValueTask AwaitTemplateRenderTask(
        ValueTask renderTask,
        ChunkedPipeWriter chunkedWriter,
        RazorSlice template)
    {
        await renderTask.ConfigureAwait(false);
        EndTemplateRendering(chunkedWriter, template);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EndTemplateRendering(
        ChunkedPipeWriter chunkedWriter,
        RazorSlice template)
    {
        chunkedWriter.Complete();
        ReturnChunkedWriter(chunkedWriter);
        template.Dispose();
    }

    private sealed class ChunkedWriterObjectPolicy :
        IPooledObjectPolicy<ChunkedPipeWriter>
    {
        public ChunkedPipeWriter Create() => new();

        public bool Return(ChunkedPipeWriter writer)
        {
            writer.Reset();
            return true;
        }
    }

    private enum RequestType
    {
        NotRecognized,
        Fortunes,
    }
}
