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


include("dataset.jl")


defaultconfig!(::ProjectTag, d::AbstractDict) = d

function config(::Type{Dict}, ::ProjectTag; kwargs...)
    d = isempty(kwargs) ? Dict{Symbol,Any}() : convert(Dict{Symbol,Any}, Dict(kwargs))
    defaultconfig!(tag, d)
end
config(tag::ProjectTag; kwargs...) = config(Dict, tag; kwargs...)


export ProjectTag, Tag, TableTag
export directory!
export @tag, @tabletag
export alltags, iscomplete, initialize!, initialize
export load, load!, save

end # module
