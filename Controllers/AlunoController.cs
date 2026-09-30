using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using PocDotNetPostgresql.Data;
using PocDotNetPostgresql.Models;
using Confluent.Kafka;
using System.Text.Json;
namespace PocDotNetPostgresql.Controllers;


[ApiController]
[Route("api/[controller]")]
public class AlunoController : ControllerBase
{
    private readonly AppDbContext _context;

    private readonly IProducer<string, string> _producer;

    private const string TopicName = "tpc_dotnet_api";

    // injecao de dependencia
    public AlunoController(AppDbContext context, IProducer<string, string> producer) {

        _context = context;
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> CreateAluno([FromBody] Alunos newAlunos) {

        if (newAlunos == null) {
            return BadRequest("Invalid product data.");
        }

        var aluno = new Alunos {
            Nome = newAlunos.Nome,
            Faltas = newAlunos.Faltas,
            Media = newAlunos.Media,
            Situacao = newAlunos.Situacao
        };

        _context.Alunos.Add(aluno);
        // Gera o Id automaticamente
        await _context.SaveChangesAsync();

        // Best practice: Return 201 Created with the location of the new resource
        return CreatedAtAction(nameof(CreateAluno), new { aluno.Id }, aluno);

    }

    ///api/Aluno
    [HttpGet]
    public async Task<IActionResult> GetAlunos() {

        var alunos = await _context.Alunos.AsNoTracking()
                             .ToListAsync();

        return Ok(alunos);

    }

    ///api/Aluno/{id}
    [HttpGet("{id:int}")]
    public async Task<IActionResult> GetAlunoById(int id)
    {

        if (await _context.Alunos.FindAsync(id) == null) {
            return NotFound();
        }


        return Ok(await _context.Alunos.FindAsync(id));

    }

    ///api/Aluno/{id}
    [HttpPut("{id:int}")]
    public async Task<IActionResult> update(int id, Alunos aluno) {

        if (id != aluno?.Id) {
            return BadRequest("Mismatched ID in URL and request body.");
        }

        _context.Entry(aluno).State = EntityState.Modified;

        try {

            await _context.SaveChangesAsync();

        } catch (DbUpdateConcurrencyException) {
            throw;
        }

        // HTTP 204 No Content
        return NoContent();

    }

    ///api/Aluno/{id}
    [HttpDelete("{id:int}")]
    public async Task<IActionResult> DeleteById(int id)
    {
        var aluno = await _context.Alunos.FindAsync(id);

        if (aluno == null) {
            return NotFound();
        }

        _context.Alunos.Remove(aluno);
        await _context.SaveChangesAsync();

        return NoContent();

    }


    [HttpPost("/producer")]
    public async Task<IActionResult> ProducerEvent([FromBody] Alunos newAlunos) {

        try {
            var messageValue = JsonSerializer.Serialize(newAlunos);

            var message = new Message<string, string>
            {
                Key = newAlunos.Id.ToString(),
                Value = messageValue
            };

            DeliveryResult<string, string> result = await _producer.ProduceAsync(TopicName,message);

            return Ok(new
            {
                Status = "Mensagem enviada com sucesso!",
                Partition = result.Partition.Value,
                Offset = result.Offset.Value
            });


        } catch(ProduceException<string,string> ex) {
            return StatusCode(500, $"Falha ao publicar no Kafka: {ex.Error.Reason}");
        }


    }
    

    }

