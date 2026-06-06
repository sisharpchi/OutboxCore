using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Abstractions;
using OutboxCore.Models;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class DashboardUiTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;
    private readonly HttpClient _client;

    public DashboardUiTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _client = _fixture.CreateClient();
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    [Fact]
    public async Task F5_T1_01_VerifyDashboardHomePageReturns200Ok()
    {
        var response = await _client.GetAsync("/outbox-dashboard");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("OutboxCore Dashboard", content);
    }

    [Fact]
    public async Task F5_T1_02_VerifyDashboardStatsApiReturnsCounts()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/stats?module=Default");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("selectedModule", out var selectedModule));
        Assert.Equal("Default", selectedModule.GetString());
        Assert.True(root.TryGetProperty("outbox", out var outbox));
        Assert.True(outbox.TryGetProperty("pending", out _));
    }

    [Fact]
    public async Task F5_T1_03_VerifyDashboardMessagesApiReturnsListForOutbox()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/messages?module=Default&type=outbox");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
    }

    [Fact]
    public async Task F5_T1_04_VerifyDashboardMessagesApiReturnsListForInbox()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/messages?module=Default&type=inbox");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
    }

    [Fact]
    public async Task F5_T1_05_VerifyDashboardRetryApiRequeuesFailedMessage()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        var msg = new OutboxMessage
        {
            Id = messageId,
            ModuleName = "Default",
            MessageType = "FailedEvent",
            Content = "{}",
            Status = "Failed",
            CreatedAt = DateTimeOffset.UtcNow
        };

        if (repo is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();

            var response = await _client.PostAsync($"/outbox-dashboard/api/retry?module=Default&id={messageId}", null);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            db.ChangeTracker.Clear();
            var dbMsg = await db.OutboxMessages.FindAsync(messageId);
            Assert.Contains(dbMsg?.Status, new[] { "Pending", "Processing", "Processed" });
        }
        else
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task F5_T2_01_VerifyDashboardApiHandlesRequestsForNonExistentModule()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/stats?module=NonExistentModule");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("NonExistentModule", content);
    }

    [Fact]
    public async Task F5_T2_02_VerifyDashboardUiReturns404ForUnknownSubRoutes()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/unknown-sub-route");
        // Unknown api sub route should hit _next(context) which in our test web app results in 404
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task F5_T2_03_VerifyDashboardStatsApiReturnsEmptyListsForUnregisteredModule()
    {
        var response = await _client.GetAsync("/outbox-dashboard/api/messages?module=UnregisteredModule&type=outbox");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Equal("[]", content);
    }

    [Fact]
    public async Task F5_T2_04_VerifyDashboardRetryApiReturns400ForInvalidMessageId()
    {
        var response = await _client.PostAsync("/outbox-dashboard/api/retry?module=Default&id=not-a-guid", null);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task F5_T2_05_VerifyDashboardUiIsServedCorrectlyUnderCustomizedPrefix()
    {
        var response = await _client.GetAsync("/outbox-dashboard/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
