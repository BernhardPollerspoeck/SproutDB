using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace SproutDB.Analyzers;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SproutDbPairingAnalyzer : DiagnosticAnalyzer
{
    private static readonly (string Map, string Add)[] Pairings =
    {
        ("MapSproutDB", "AddSproutDB"),
        ("MapSproutDBAdmin", "AddSproutDBAdmin"),
    };

    private static readonly DiagnosticDescriptor Rule = new(
        id: "SPROUT001",
        title: "Missing Add registration for Map call",
        messageFormat: "'{0}' requires a matching '{1}' call on the service collection",
        category: "SproutDB",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        var methodName = GetMethodName(invocation);
        if (methodName is null)
            return;

        foreach (var (map, add) in Pairings)
        {
            if (methodName != map)
                continue;

            // Search all syntax trees in the compilation for the matching Add call
            foreach (var tree in context.Compilation.SyntaxTrees)
            {
                var root = tree.GetRoot(context.CancellationToken);
                if (ContainsCall(root, add))
                    return;
            }

            context.ReportDiagnostic(
                Diagnostic.Create(Rule, invocation.GetLocation(), map, add));
        }
    }

    private static bool ContainsCall(SyntaxNode root, string methodName)
    {
        foreach (var node in root.DescendantNodes())
        {
            if (node is InvocationExpressionSyntax inv && GetMethodName(inv) == methodName)
                return true;
        }
        return false;
    }

    private static string? GetMethodName(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression switch
        {
            MemberAccessExpressionSyntax memberAccess => memberAccess.Name.Identifier.Text,
            IdentifierNameSyntax identifier => identifier.Identifier.Text,
            _ => null,
        };
    }
}
