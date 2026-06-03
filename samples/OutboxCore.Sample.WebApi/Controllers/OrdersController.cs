using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using OutboxCore.Sample.WebApi.Data;
using OutboxCore.Sample.WebApi.Domain;

namespace OutboxCore.Sample.WebApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly ApplicationDbContext _dbContext;

    public OrdersController(ApplicationDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public record CreateOrderRequest(string CustomerName, decimal TotalAmount);

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.CustomerName) || request.TotalAmount <= 0)
        {
            return BadRequest("Invalid order details");
        }

        var order = Order.Create(request.CustomerName, request.TotalAmount);

        _dbContext.Orders.Add(order);
        await _dbContext.SaveChangesAsync();

        return CreatedAtAction(nameof(GetOrder), new { id = order.Id }, order);
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetOrder(Guid id)
    {
        var order = await _dbContext.Orders.FindAsync(id);
        if (order == null)
        {
            return NotFound();
        }
        return Ok(order);
    }
}
