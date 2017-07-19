#=========================================================================================
    Let ϕ be a pull morphism and θ be a push moprhism.

    For each funtion ϕᵢ ∈ ϕ, we get
        ϕᵢ(v₁,...,vₙ) → Any
    where the vs are columns from the source specified by the column numbers.
    (these are either combined into a tuple or vector)

    For each function θᵢ ∈ θ, we get
        θᵢ(V::Matrix) → Union{Matrix,Vector}
    at which point the columns of the resulting Matrix are put into the columns
    of the sink according to the column numbers provided.  If the result is a vector,
    it is assumed that this is a vector of vectors which are to be placed in columns.

    Note that the "identity" single-function Morphisms are created with
    ```
    Morphism{Pull}(src, src_cols, identity)
    Morphism{Push}(snk, snk_cols, identity)
    ```
    Where we have defined `identity` of multiple arguments to return a tuple of those
    arguments.  (See utils.jl.)
=========================================================================================#



struct Pull <: MapDirection end
struct Push <: MapDirection end
export Pull, Push


# helper functions used by constructor
_handle_col_arg(sch::Data.Schema, c) = throw(ArgumentError("$c is not a valid column name."))

_handle_col_arg(sch::Data.Schema, c::Integer) = c
_handle_col_arg(sch::Data.Schema, c::String) = sch[c]
_handle_col_arg(sch::Data.Schema, c::Symbol) = _handle_col_arg(sch, string(c))

_handle_col_args(sch::Data.Schema, t::Tuple) = tuple((_handle_col_arg(sch,c) for c ∈ t)...)

function _handle_col_args(sch::Data.Schema, v::AbstractVector)
    tuple((_handle_col_arg(sch,c) for c ∈ v)...)
end

function _handle_col_args(::Type{Vector}, sch::Data.Schema, v::AbstractVector)
    Int[_handle_col_arg(sch,c) for c ∈ v]
end


"""
# Type: `Morphism{T<:MapDirection}`

This is a type for wrapping functions which transfer data to and from a tabular data
format which implements the [DataStreams](https://github.com/JuliaData/DataStreams.jl)
interface.  The primary functionality involves creating a function which takes an
index as an argument and either feeds data to a data sink or extracts data from a source,
while passing the data through a function.


## Constructors

```julia
Morphism{T<:MapDirection}(s, sch::Data.Schema, cols::AbstractVector,
                          funcs::AbstractVector)
Morphism{T<:MapDirection}(s, cols::AbstractVector, funcs::AbstractVector)
Morphism{T<:MapDirection}(s, cols::AbstractVector, f::Function)`
```

## Arguments

- `T<:MapDirection`: This is either `Pull` or `Push`. A `Morphism{Pull}`
    is for extracting data from a table, a `Morphism{Push}` is for injecting
    data into a table.
- `s`: The source (in the `Pull` case) or sink (in the `Push` case). This must
    be a tabular data format implementing the `DataStreams` interface.
- `sch`: A `Data.Schema` schema generated from `s`.  If this argument is omitted, a
    schema will be generated within the `Morphism` constructor.
- `cols`: A vector of tuples, each containing column designations.  These designations can
    be either `Integer`, `Symbol` or `String`.  If only a single function is being passed
    to the constructor, one can instead pass a vector of column designations.
- `funcs`: Functions to be applied to the data passing into, or being taken out of `s`.
    The `cols` argument should contain a tuple for each such function.
- `f`: If only a single function is being used, one can pass a single function which is
    not wrapped in a vector.  In this case, `cols` should contain column designations
    rather than tuples.


## Notes

`Morphism` is the base type on which other objects such as `Harvester` `Sower` and
`StreamFilter` in `TheDataMustFlow` are based.  Abstractly, it represents any function
applied to tabular data composed with a data transfer.  See the function constructor
function `morphism` for more details.

***TODO:*** Macros are coming!


## Examples

```julia
# this extracts columns 1,2,3 and does nothing to them
I = Morphism{Pull}(data, [1,2,3], identity)  # we have overriden identity to return
                                             # tuples for multiple arguments
i = morphism(I)  # this is the function for pulling data
k, = i(1:10)  # this returns rows 1 through 10. note that always returns a tuple
              # here k is itself a tuple

f(x,y) = x .+ y
g(x,y) = x .- y
M = Morphism{Pull}(src, [(1,2), (3,4)], [f, g])
m = morphism(M)  # again, this is the function for pulling data
α, β = m(8:12)  # here α is the sum of columns 1,2 rows 8 through 12
                # β is the difference of columns 3,4 rows 8 through 12

# this inserts exactly what you give it into columns 3, 4
I = Morphism{Push}(data, [3,4], identity)
i = morphism(M)  # this is the function for feeding data
i(1:3, ones(3,2))  # this inserts ones into columns 3,4 rows 1 through 3

h₁(x) = [x+1, x+2]
h₂(x) = [x+3, x+4]
m = morphism(Push, data, [(3,4), (5,6)], [h₁,h₂])  # you can bypass the Morphism constructor
m(4:6, ones(3), 2ones(3))  # this puts numbers into columns 3,4,5,6
```
Note that the functions passed to `Pull` `Morphism`s accept as many arguments as there are
column specified for them and that `Push` `Morphism`s can take functions that return either
matrices or vectors of vectors (or tuples).  See `morphism` for more information.
"""
struct Morphism{T<:MapDirection} <: AbstractMorphism{T}
    s::Any  # this can be either a source or a sink
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    # may add a new field for conversions

    function Morphism{T}(s, sch::Data.Schema,
                         cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function}) where T
        cols = Tuple[_handle_col_args(sch, t) for t ∈ cols]
        new(s, sch, cols, funcs)
    end
    function Morphism{T}(s, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function}) where T
        Morphism{T}(s, Data.schema(s), cols, funcs)
    end

    # constructor for using single function
    function Morphism{T}(s, sch::Data.Schema, cols::AbstractVector, f::Function) where T
        Morphism{T}(s, sch, Tuple[tuple(cols...)], Function[f])
    end
    function Morphism{T}(s, cols::AbstractVector, f::Function) where T
        Morphism{T}(s, Data.schema(s), cols, f)
    end

    # blank constructors
    function Morphism{T}(s, sch::Data.Schema) where T
        Morphism{T}(s, sch, Vector{Tuple}(), Vector{Function}())
    end
    function Morphism{T}(s) where T
        Morphism{T}(s, Data.schema(s))
    end
end
export Morphism


function addfunc!(m::Morphism, cols, func::Function)
    push!(m.cols, _handle_col_args(m.schema, cols))
    push!(m.funcs, func)
end


"""
    morphism(M::AbstractMorphism)

This returns a function that executes the transformations specified by `M`.  Note that
one can pass the arguments to the cunstructor of a `Morphism` to `morphism` directly,
obviating the need to write the `Morphism` constructor separately.  The function returned
by `morphism` accepts different arguments depending on whether `M` is `Push` or `Pull`

### `Pull`
If the `AbstractMorphism` passed to `morphism` is of `Pull` type, then the function `m`
returned by `morphism` will accept only a single argument.  That argument must be an
`AbstractVector{<:Integer}` containing the rows of the table which are to be pulled from.
The functions passed to `M` will be applied to the appropriate columns, only for the
rows specified.  See `Morphism` for more detail.

### `Push`
If the `AbstractMorphism` passed to `morphism` is of `Push` type, then the function `m`
returned by `morphism` will accept an `AbstractVector{<:Integer}` index argument followed
by one argument for each function `M` was constructed with.  See `Morphism` for more details.
"""
function morphism(::Type{D}, s, sch::Data.Schema,
                  cols::AbstractVector{<:Tuple},
                  funcs::AbstractVector{<:Function}, ::Type{R}=Tuple) where {D<:MapDirection,R}
    morphism(Morphism{D}(s, sch, cols, funcs), R)
end
function morphism(::Type{D}, s, cols::AbstractVector{<:Tuple},
                  funcs::AbstractVector{<:Function}, ::Type{R}=Tuple) where {D<:MapDirection,R}
    morphism(Morphism{D}(s, cols, funcs), R)
end

# single function constructor
function morphism(::Type{D}, s, cols::AbstractVector, f::Function,
                  ::Type{R}=Tuple) where {D<:MapDirection,R}
    morphism(Morphism{D}(s, cols, f))
end

export morphism


# TODO consider adding parameter arguments to functions
#=========================================================================================
    <Pull>
=========================================================================================#
function _morphism_return_func(m::AbstractMorphism{Pull}, colstypes::Vector,
                               colmaps::Vector, nargs::Vector, ::Type{Vector})
    ℓ = length(m.cols)
    function (idx::AbstractVector{<:Integer})
        o = Vector{Any}(ℓ)
        cols = Any[streamfrom(m.s, Data.Column, dtype, idx, c) for (c,dtype) ∈ colstypes]
        for i ∈ 1:ℓ
            o[i] = m.funcs[i]((cols[colmaps[i][j]] for j ∈ 1:nargs[i])...)
        end
        o
    end
end
function _morphism_return_func(m::AbstractMorphism{Pull}, colstypes::Vector,
                               colmaps::Vector, nargs::Vector, ::Type{Tuple})
    ℓ = length(m.cols)
    function (idx::AbstractVector{<:Integer})
        cols = Any[streamfrom(m.s, Data.Column, dtype, idx, c) for (c,dtype) ∈ colstypes]
        tuple((m.funcs[i]((cols[colmaps[i][j]] for j ∈ 1:nargs[i])...)
               for i ∈ 1:ℓ)...)
    end
end

function morphism(m::AbstractMorphism{Pull}, ::Type{R}=Tuple) where R
    allcols = collect(∪((Set(a) for a ∈ m.cols)...))
    alltypes = coltypes(m.schema, allcols)
    colstypes = collect(zip(allcols, alltypes))
    colmaps = [Dict(i=>findfirst(allcols, a[i]) for i ∈ 1:length(a)) for a ∈ m.cols]
    nargs = [length(a) for a ∈ m.cols]

    _morphism_return_func(m, colstypes, colmaps, nargs, R)
end
#=========================================================================================
    </Pull>
=========================================================================================#



#=========================================================================================
    <Push>
=========================================================================================#
function _pushforward_column(m::AbstractMorphism{Push},
                             col::AbstractVector{T}, ::Type{To},
                             idx::AbstractVector{<:Integer},
                             tocol::Integer) where {T,To}
    streamto!(m.s, Data.Column, convert(To, col), idx, tocol, m.schema)
end

_get_column(X::Matrix, j::Integer) = X[:,j]
_get_column(X::Vector, j::Integer) = X[j]  # for cases when we use a vector of vectors
_get_column(X::Tuple, j::Integer) = X[j]   # for cases when we use a tuple of vectors


function morphism(m::AbstractMorphism{Push})
    ℓ = length(m.funcs)
    totypes = coltypes(m.schema, 1:size(m.schema,2))

    # length of args here should be equal to number of functions
    function (idx::AbstractArray{<:Integer}, args::Union{AbstractArray,Tuple}...)
        for i ∈ 1:ℓ
            X = m.funcs[i](args[i])
            for (j,c) ∈ enumerate(m.cols[i])
                _pushforward_column(m, _get_column(X,j), totypes[c], idx, c)
            end
        end
    end
end
#=========================================================================================
    </Push>
=========================================================================================#



