using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OutboxTestInmemory.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "outbox_messages",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    tenant_id = table.Column<string>(
                        type: "character varying(128)",
                        maxLength: 128,
                        nullable: false
                    ),
                    event_type = table.Column<string>(
                        type: "character varying(128)",
                        maxLength: 128,
                        nullable: false
                    ),
                    payload = table.Column<string>(type: "text", nullable: false),
                    metadata = table.Column<string>(type: "text", nullable: true),
                    status = table.Column<int>(type: "integer", nullable: false),
                    retry_count = table.Column<int>(type: "integer", nullable: false),
                    available_at = table.Column<DateTime>(
                        type: "timestamp with time zone",
                        nullable: false
                    ),
                    created_at = table.Column<DateTime>(
                        type: "timestamp with time zone",
                        nullable: false
                    ),
                    processed_at = table.Column<DateTime>(
                        type: "timestamp with time zone",
                        nullable: true
                    ),
                    claimed_at = table.Column<DateTime>(
                        type: "timestamp with time zone",
                        nullable: true
                    ),
                    worker_id = table.Column<string>(
                        type: "character varying(64)",
                        maxLength: 64,
                        nullable: true
                    ),
                    last_error = table.Column<string>(type: "text", nullable: true),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_outbox_messages", x => x.id);
                }
            );

            migrationBuilder.CreateIndex(
                name: "ix_outbox_messages_pending_due",
                table: "outbox_messages",
                columns: new[] { "status", "available_at" },
                filter: "status = 0"
            );

            migrationBuilder.CreateIndex(
                name: "ix_outbox_messages_processing_claimed",
                table: "outbox_messages",
                columns: new[] { "status", "claimed_at" },
                filter: "status = 1"
            );

            migrationBuilder.CreateIndex(
                name: "ix_outbox_messages_tenant_id",
                table: "outbox_messages",
                column: "tenant_id"
            );
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "outbox_messages");
        }
    }
}
