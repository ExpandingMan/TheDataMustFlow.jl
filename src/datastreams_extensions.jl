
vectortype{T}(::Type{T}) = Vector{T}
vectortype{T}(::Type{Nullable{T}}) = NullableVector{T}

coltype(sch::Data.Schema, i::Integer) = vectortype(Data.types(sch)[i])
function coltypes(sch::Data.Schema, cols)
    DataType[coltype(sch, c) for c ∈ cols]
end
coltypes(sch::Data.Schema) = coltypes(sch, 1:length(Data.header(sch)))

Base.eltype(sch::Data.Schema, i::Integer) = Data.types(sch)[i]
Base.eltype(sch::Data.Schema, c::String) = eltype(sch, sch[c])
Base.eltype(sch::Data.Schema, c::Symbol) = eltype(sch, string(c))

# this will hopefully get implemented in DataStreams some day
function streamfrom{T}(s, ::Type{Data.Column}, ::Type{Vector{T}},
                       rows::AbstractVector{<:Integer}, col::Integer)
    [Data.streamfrom(s, Data.Field, T, i, col) for i ∈ rows]::Vector{T}
end
function streamfrom{T}(s, ::Type{Data.Column}, ::Type{NullableVector{T}},
                       rows::AbstractVector{<:Integer}, col::Integer)
    # TODO wow, this is bad
    # can't fix without updating DataStreams standard
    o = [Data.streamfrom(s, Data.Field, Nullable{T}, i, col) for i ∈ rows]
    convert(NullableArray, o)::NullableVector{T}
end


# TODO may have to include streamto! schema argument
# again, hopefully these will be added to DataStreams at some point
function streamto!{T}(sink, ::Type{Data.Column}, v::T, rows::AbstractVector{<:Integer},
                      col::Integer, sch::Data.Schema)
    for (i, row) ∈ enumerate(rows)
        Data.streamto!(sink, Data.Field, v[i], row, col, sch)
    end
end


# TODO make types more configurable
function coerce{T,U}(s, ::Type{T}, ::Type{U}, idx::AbstractVector{<:Integer}, c::Integer)
    convert(Vector{T}, streamfrom(s, Data.Column, Vector{U}, idx, c))
end
function coerce{T,U}(s, ::Type{T}, ::Type{Nullable{U}}, idx::AbstractVector{<:Integer},
                     c::Integer)
    convert(Vector{T}, streamfrom(s, Data.Column, NullableVector{U}, idx, c))
end


# TODO user needs to have option about whether to use this
_apply_nolift(f::Function, v::AbstractVector) = f.(v)
function _apply_nolift(f::Function, v::NullableVector)
    o = Vector{Bool}(length(v))
    for i ∈ 1:length(v)
        o[i] = v.isnull[i] ? false : f(v.values[i])
    end
    o
end

function sift{T}(s, f::Function, ::Type{T}, i::Integer, ab::AbstractVector{<:Integer})
    _apply_nolift(f, streamfrom(s, Data.Column, T, ab, i))::AbstractVector{Bool}
end


