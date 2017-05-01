

# TODO for now everything is one row at a time, at some point must do blocks


"""
# Type: `Harvester`

**TODO** Documentation!!!
"""
struct Harvester <: AbstractMorphism{PullBack}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    function Harvester(s, sch::Data.Schema,
                       cols::AbstractVector{<:Tuple},
                       funcs::AbstractVector{<:Function})
        cols = Tuple[_handle_col_args(sch, t) for t ∈ cols]
        new(s, sch, cols, funcs)
    end
    function Harvester(s, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function})
        new(s, Data.schema(s), cols, funcs)
    end
end
export Harvester


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
# this returns the core function which just converts and concatenates
function _create_hcat_convert{T}(::Type{T})
    (vs::AbstractVector...) -> hcat((convert(Vector{T}, v) for v ∈ vs)...)
end

function Harvester{T}(s, ::Type{T}, sch::Data.Schema, matrix_cols::AbstractVector{Symbol}...)
    cols = Tuple[tuple(colidx(sch, mc)...) for mc ∈ matrix_cols]
    funcs = Function[_create_hcat_convert(T) for c ∈ cols]
    Harvester(s, sch, cols, funcs)
end
function Harvester{T}(s, ::Type{T}, matrix_cols::AbstractVector{Symbol}...)
    Harvester(s, T, Data.schema(s), matrix_cols...)
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


"""
    harvester(h::Harvester)

**TODO** Documentation!
"""
harvester(h::Harvester) = morphism(h)
function harvester{T}(s, ::Type{T}, matrix_cols::AbstractVector{Symbol}...)
    harvester(Harvester(s, T, matrix_cols...))
end
export harvester


function batchiter(idx::AbstractVector{<:Integer}, h::Harvester;
                   batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    batchiter(harvester(h), idx, batch_size)
end




