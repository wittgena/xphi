package subst.ktory

fun convertLmpToDsl(comments: List<LmpComment>): List<DslNode> =
    comments.map {
        DslNode(
            phase = it.keyword.removePrefix("@").lowercase().split(".").first(),
            label = it.content.trim(),
            location = it.location
        )
    }

fun groupDslByFunction(
    dslNodes: List<DslNode>,
    functions: List<FunctionInfo>
): List<DslExBundle> =
    functions.map { fn ->
        DslExBundle(
            function = fn.name,
            dslNodes = dslNodes.filter { it.location == fn.name },
            calls = fn.calls
        )
    }

/** @contract.projection **/
fun toContract(
    functions: List<FunctionInfo>,
    classes: List<ClassInfo>,
    annotations: Set<String>
): List<Contract> {
    val contracts = mutableListOf<Contract>()

    functions.forEach { fn ->
        contracts += Contract(
            kind = "function",
            name = fn.name,
            features = annotations.toList(),
            refs = fn.calls,
            location = "${fn.startLine}:${fn.endLine}" // 라인 범위로 수정!
        )
    }

    classes.forEach { cls ->
        contracts += Contract(
            kind = "class",
            name = cls.name,
            features = annotations.toList(),
            refs = emptyList(),
            location = "${cls.startLine}:${cls.endLine}" // 라인 범위로 수정!
        )
    }

    return contracts
}
