using Microsoft.EntityFrameworkCore;
using PocDotNetPostgresql.Models;

namespace PocDotNetPostgresql.Data;


public class AppDbContext : DbContext
{
	public AppDbContext(DbContextOptions<AppDbContext> options) : base(options){

	}

	public DbSet<Alunos> Alunos { get; set;}

	
}

