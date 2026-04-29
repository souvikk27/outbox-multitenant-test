namespace OutboxTestInmemory.Sample.Email;

public sealed record EmailPayload(string To, string Subject, string Body, int Index);
