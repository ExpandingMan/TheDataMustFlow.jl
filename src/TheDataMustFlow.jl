module TheDataMustFlow

using MacroTools


abstract type DataRemote end

abstract type AbstractMetaData end
abstract type AbstractDataSet end
abstract type AbstractInput end
abstract type AbstractProblem end

abstract type ProjectTag end
abstract type Tag end
abstract type TableTag <: Tag end


include("dataremote.jl")
include("dataset.jl")


export DataRemote, ProjectTag, Tag, TableTag
export directory!
export @tag, @tabletag
export alltags, iscomplete, initialize!, initialize
export load, load!, save

end # module
