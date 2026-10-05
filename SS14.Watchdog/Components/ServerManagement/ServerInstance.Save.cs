using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace SS14.Watchdog.Components.ServerManagement;

public sealed partial class ServerInstance
{
    private readonly object _saveLock = new();
    private DateTime? _saveStarted;
    private DateTime _saveLastProgress;
    private string? _saveReason;

    private TimeSpan SaveTimeout => TimeSpan.FromSeconds(_instanceConfig.SaveTimeoutSeconds);
    private TimeSpan SaveProgressTimeout => TimeSpan.FromSeconds(_instanceConfig.SaveProgressTimeoutSeconds);

    public Task SaveStateReceived(ServerSaveState state, bool ok, string? reason)
    {
        var now = DateTime.Now;
        TimeSpan elapsed;
        lock (_saveLock)
        {
            switch (state)
            {
                case ServerSaveState.Begin:
                    _saveStarted = now;
                    _saveLastProgress = now;
                    _saveReason = reason;
                    elapsed = TimeSpan.Zero;
                    break;

                case ServerSaveState.Progress:
                    _saveStarted ??= now;
                    _saveReason ??= reason;
                    _saveLastProgress = now;
                    elapsed = now - _saveStarted.Value;
                    break;

                default:
                    elapsed = _saveStarted is { } started ? now - started : TimeSpan.Zero;
                    _saveStarted = null;
                    _saveReason = null;
                    break;
            }
        }

        switch (state)
        {
            case ServerSaveState.Begin:
                _logger.LogInformation("{Key}: world save started ({Reason})", Key, reason);
                break;

            case ServerSaveState.End when ok:
                _logger.LogInformation("{Key}: world save finished in {Elapsed:0.0}s ({Reason})",
                    Key, elapsed.TotalSeconds, reason);
                break;

            case ServerSaveState.End:
                _logger.LogError("{Key}: world save FAILED after {Elapsed:0.0}s ({Reason})",
                    Key, elapsed.TotalSeconds, reason);
                _notificationManager.SendNotification(
                    $"Server `{Key}` reported a failed world save ({reason}). Check server logs.");
                break;
        }

        if (state != ServerSaveState.End && elapsed >= SaveTimeout)
        {
            _logger.LogWarning(
                "{Key}: world save has been running for {Elapsed:0}s, past SaveTimeoutSeconds; no longer holding the process",
                Key, elapsed.TotalSeconds);
            return Task.CompletedTask;
        }

        _commandQueue.Writer.TryWrite(new CommandServerPing());
        return Task.CompletedTask;
    }

    private bool IsSaveHoldingProcess(out TimeSpan elapsed)
    {
        var now = DateTime.Now;
        lock (_saveLock)
        {
            if (_saveStarted is not { } started)
            {
                elapsed = TimeSpan.Zero;
                return false;
            }

            elapsed = now - started;
            return elapsed < SaveTimeout && now - _saveLastProgress < SaveProgressTimeout;
        }
    }

    private bool TryTakeInterruptedSave(out string? reason, out TimeSpan elapsed)
    {
        lock (_saveLock)
        {
            reason = _saveReason;
            elapsed = _saveStarted is { } started ? DateTime.Now - started : TimeSpan.Zero;
            var interrupted = _saveStarted != null;
            _saveStarted = null;
            _saveReason = null;
            return interrupted;
        }
    }
}
