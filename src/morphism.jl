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
    Morphism{PullBack}(src, src_cols, identity)
    Morphism{PushForward}(snk, snk_cols, identity)
    ```
    Where we have defined `identity` of multiple arguments to return a tuple of those
    arguments.  (See utils.jl.)
=========================================================================================#



struct PullBack <: MapDirection end
struct PushForward <: MapDirection end
export PullBack, PushForward


# helper functions used by constructor
_handle_col_arg(sch::Data.Schema, c::Integer) = c
_handle_col_arg(sch::Data.Schema, c::String) = sch[c]
_handle_col_arg(sch::Data.Schema, c::Symbol) = _handle_col_arg(sch, string(c))

_handle_col_args(sch::Data.Schema, t::Tuple) = tuple((_handle_col_arg(sch,c) for c ∈ t)...)


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
end
export Morphism



function morphism{D<:MapDirection,R}(::Type{D}, s, sch::Data.Schema,
                                     cols::AbstractVector{<:Tuple},
                                     funcs::AbstractVector{<:Function}, ::Type{R}=Tuple)
    morphism(Morphism{D}(s, sch, cols, funcs), R)
end
function morphism{D<:MapDirection,R}(::Type{D}, s, cols::AbstractVector{<:Tuple},
                                     funcs::AbstractVector{<:Function}, ::Type{R}=Tuple)
    morphism(Morphism{D}(s, cols, funcs), R)
end

# single function constructor
function morphism{D<:MapDirection,R}(::Type{D}, s, cols::AbstractVector, f::Function,
                                     ::Type{R}=Tuple)
    moprhism(Morophism{D}(s, cols, f))
end



# TODO consider adding parameter arguments to functions
#=========================================================================================
    <PullBack>
=========================================================================================#
function _morphism_return_func(m::AbstractMorphism{PullBack}, colstypes::Vector,
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
function _morphism_return_func(m::AbstractMorphism{PullBack}, colstypes::Vector,
                               colmaps::Vector, nargs::Vector, ::Type{Tuple})
    ℓ = length(m.cols)
    function (idx::AbstractVector{<:Integer})
        cols = Any[streamfrom(m.s, Data.Column, dtype, idx, c) for (c,dtype) ∈ colstypes]
        tuple((m.funcs[i]((cols[colmaps[i][j]] for j ∈ 1:nargs[i])...)
               for i ∈ 1:ℓ)...)
    end
end

function morphism{R}(m::AbstractMorphism{PullBack}, ::Type{R}=Tuple)
    allcols = collect(∪((Set(a) for a ∈ m.cols)...))
    alltypes = coltypes(m.schema, allcols)
    colstypes = collect(zip(allcols, alltypes))
    colmaps = [Dict(i=>findfirst(allcols, a[i]) for i ∈ 1:length(a)) for a ∈ m.cols]
    nargs = [length(a) for a ∈ m.cols]

    _morphism_return_func(m, colstypes, colmaps, nargs, R)
end
#=========================================================================================
    </PullBack>
=========================================================================================#



#=========================================================================================
    <PushForward>
=========================================================================================#
function _pushforward_column{T,To}(m::AbstractMorphism{PushForward},
                                   col::AbstractVector{T}, ::Type{To},
                                   idx::AbstractVector{<:Integer},
                                   tocol::Integer)
    streamto!(m.s, Data.Column, convert(To, col), idx, tocol, m.schema)
end

_get_column(X::Matrix, j::Integer) = X[:,j]
_get_column(X::Vector, j::Integer) = X[j]  # for cases when we use a vector of vectors
_get_column(X::Tuple, j::Integer) = X[j]   # for cases when we use a tuple of vectors


function morphism(m::AbstractMorphism{PushForward})
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
    </PushForward>
=========================================================================================#



