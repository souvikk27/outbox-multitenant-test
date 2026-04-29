using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OutboxTestInmemory.Outbox.DependencyInjection;
using OutboxTestInmemory.Outbox.Telemetry;
using OutboxTestInmemory.Sample.Email;
using OutboxTestInmemory.Sample.Endpoints;
using OutboxTestInmemory.Sample.Persistence;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Postgres connection string missing. Set ConnectionStrings__DefaultConnection.");

// Application's own DbContext — also hosts the outbox table (transactional enqueue).
builder.Services.AddDbContextFactory<AppDbContext>(options =>
    options
        .UseNpgsql(connectionString, npg => npg.EnableRetryOnFailure(maxRetryCount: 3))
        .UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking));

// Outbox library — single fluent registration.
builder.Services
    .AddOutbox<AppDbContext>(o => builder.Configuration.GetSection("Outbox").Bind(o))
    .AddProcessing()
    .AddHandler<EmailPayload, EmailHandler>("email");

// Health
builder.Services
    .AddHealthChecks()
    .AddNpgSql(connectionString, name: "postgres", tags: ["ready"])
    .AddOutboxBacklogHealthCheck(tags: "ready");

// Telemetry
builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("OutboxTestInmemory"))
    .WithMetrics(m => m
        .AddMeter(OutboxDiagnostics.MeterName)
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddOtlpExporter())
    .WithTracing(t => t
        .AddSource(OutboxDiagnostics.ActivitySourceName)
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter());

builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapOutboxAdmin();

app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.Run();
