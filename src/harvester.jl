

"""
# Type: `Harvester <: AbstractMorphism{Pull}`

This is a type for pulling data from a tabular data source that implements the `DataStreams`
interface in a format amenable to machine learning input (a simple array).

## Constructors

```julia
Harvester(s, ::Type{T}, sch::Data.Schema, matrix_cols::AbstractVector{Symbol}...;
          null_replacement=nothing)
Harvester(s, ::Type{T}, matrix_cols::AbstractVector{Symbol}...;
          null_replacement=nothing)
```

## Arguments

- `s`: The tabular data source to pull data from. Must implement the `DataStreams` interface.
- `T`: The element type of the matrix returned by the harvester.  In most cases this will
    be either `Float32` or `Float64`.
- `sch`: A `Data.Schema` schema for `s`.  If this is not provided, it will be generated.
- `matrix_cols`: A variable length argument. The function created by the `Harvester` will
    return a matrix for each `matrx_cols` argument.
- `null_replacement`: A function or value for replacing nulls. Can only provide a zero-
    argument function which will be called for every null it replaces. Alternatively, a
    value will replace all nulls.  If `nothing`, no null substitution will be attempted.


## Examples

```julia
h = Harvester(src, Float32, [:A, :B], [:C, :D])
harvest = harvester(h)
X, y = harvest(1000:1200)  # X and y are matrices produced from rows 1000 through 1200

# can bypass constructor; replace nulls with random numbers in [0, 1]
harvest = harvester(src, Float32, [:A, :B], null_replacement=rand)
X, = harvest(1:10^6)  # note that these always return tuples
```
"""
struct Harvester <: AbstractMorphism{Pull}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    function Harvester(s, sch::Data.Schema,
                       cols::AbstractVector{<:Tuple},
                       funcs::AbstractVector{<:Function})
        cols = Tuple[_handle_col_args(sch, t) for t ∈ cols]  # this should usually be redundant
        new(s, sch, cols, funcs)
    end
    function Harvester(s, cols::AbstractVector{<:Tuple},
                       funcs::AbstractVector{<:Function})
        Harvester(s, Data.schema(s), cols, funcs)
    end
end
export Harvester


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function _handle_category_dict(sch::Data.Schema, cols::Tuple, dict::Dict)::Dict{Int,CategoricalPool}
    Dict{Int,CategoricalPool}(findfirst(cols, _handle_col_arg(sch, k))=>v for (k,v) ∈ dict)
end

# this returns the core function which just converts and concatenates
function _create_hcat_convert(::Type{T}, ::Void, cat::Dict) where T
    function (vs::AbstractVector...)
        hcat((coerce(T, v, get(cat, i, nothing)) for (i,v) ∈ enumerate(vs))...)
    end
end

function _create_hcat_convert(::Type{T}, nullfunc::Function, cat::Dict) where T
    function (vs::AbstractVector...)
        hcat((coerce(T, v, nullfunc, get(cat, i, nothing)) for (i,v) ∈ enumerate(vs))...)
    end
end

function Harvester(s, sch::Data.Schema, ::Type{T}, matrix_cols::AbstractVector...;
                   null_replacement=nothing, categories::Dict=Dict()) where T
    cols = Tuple[_handle_col_args(sch, tuple(mc...)) for mc ∈ matrix_cols]
    categories = _handle_category_dict(sch, cols[1], categories)
    funcs = Function[_create_hcat_convert(T, null_replacement, categories) for c ∈ cols]
    Harvester(s, sch, cols, funcs)
end

function Harvester(s, ::Type{T}, matrix_cols::AbstractVector...;
                   null_replacement=nothing, categories::Dict=Dict()) where T
    Harvester(s, Data.schema(s), T, matrix_cols..., null_replacement=null_replacement,
              categories=categories)
end

function Harvester(svr::Surveyor, ::Type{T}, matrix_cols::AbstractVector...;
                   null_replacement=nothing) where T
    cats = getpool(Dict, svr)
    Harvester(svr.s, svr.schema, T, matrix_cols..., null_replacement=null_replacement,
              categories=cats)
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


"""
    harvester(h::Harvester)

Returns a function `harvest(idx)` which will return matrices generated from the rows
specified by `idx`.
"""
harvester(h::Harvester) = morphism(h)
function harvester(s, ::Type{T}, matrix_cols::AbstractVector...; null_replacement=nothing,
                   categories::Dict=Dict()) where T
    harvester(Harvester(s, T, matrix_cols..., null_replacement=null_replacement, categories=categories))
end
function harvester(svr::Surveyor, ::Type{T}, matrix_cols::AbstractVector...;
                   null_replacement=nothing) where T
    harvester(Harvester(svr, T, matrix_cols..., null_replacement=null_replacement))
end
export harvester


function batchiter(idx::AbstractVector{<:Integer}, h::Harvester;
                   batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    batchiter(harvester(h), idx, batch_size)
end




