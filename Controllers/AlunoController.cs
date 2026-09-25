using Microsoft.AspNetCore.Mvc;
using PocDotNetPostgresql.Data;
using PocDotNetPostgresql.Models;
namespace PocDotNetPostgresql.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AlunoController : ControllerBase
{
	private readonly AppDbContext _context;

    // O EF Core injeta o contexto automaticamente aqui
    public AlunoController(AppDbContext context) {

		_context = context;
	}

	[HttpPost]
	public async Task<IActionResult> CreateAluno([FromBody] Alunos newAlunos) {

		if (newAlunos == null) {
			return BadRequest("Invalid product data.");
		}

		var aluno = new Alunos{
			Nome = newAlunos.Nome,
			Faltas = newAlunos.Faltas,
			Media = newAlunos.Media,
            Situacao = newAlunos.Situacao
		};

		_context.Alunos.Add(aluno);
        // Gera o Id automaticamente
        await _context.SaveChangesAsync();

        // Best practice: Return 201 Created with the location of the new resource
        return CreatedAtAction(nameof(CreateAluno), new { aluno.Id}, aluno);

	}
	
}

