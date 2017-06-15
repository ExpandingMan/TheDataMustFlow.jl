

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
        cols = Tuple[_handle_col_args(sch, t) for t ∈ cols]
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
_coerce_vec{T}(::Type{T}, v::AbstractVector, val) = convert(Vector{T}, v)
_coerce_vec{T}(::Type{T}, v::AbstractVector, ::Type{Void}) = convert(Vector{T}, v)
function _coerce_vec{T}(::Type{T}, v::NullableVector, f::Function)
    ℓ = length(v)
    o = Vector{T}(ℓ)
    for i ∈ 1:ℓ
        v.isnull[i] ? (o[i] = f()) : (o[i] = v.values[i])
    end
    o
end
_coerce_vec{T}(::Type{T}, v::NullableVector, val) = _coerce_vec(T, v, () -> val)
_coerce_vec{T}(::Type{T}, v::NullableVector, ::Type{Void}) = convert(Vector{T}, v)


# this returns the core function which just converts and concatenates
function _create_hcat_convert{T}(::Type{T}, val)
    (vs::AbstractVector...) -> hcat((_coerce_vec(T, v, val) for v ∈ vs)...)
end

function Harvester{T}(s, ::Type{T}, sch::Data.Schema, matrix_cols::AbstractVector...;
                      null_replacement=nothing)
    cols = Tuple[tuple(colidx(sch, mc)...) for mc ∈ matrix_cols]
    funcs = Function[_create_hcat_convert(T, null_replacement) for c ∈ cols]
    Harvester(s, sch, cols, funcs)
end

function Harvester{T}(s, ::Type{T}, matrix_cols::AbstractVector...;
                      null_replacement=nothing)
    Harvester(s, T, Data.schema(s), matrix_cols..., null_replacement=null_replacement)
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
function harvester{T}(s, ::Type{T}, matrix_cols::AbstractVector...)
    harvester(Harvester(s, T, matrix_cols...))
end
export harvester


function batchiter(idx::AbstractVector{<:Integer}, h::Harvester;
                   batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    batchiter(harvester(h), idx, batch_size)
end




