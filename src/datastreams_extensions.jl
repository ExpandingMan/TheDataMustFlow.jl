
vectortype{T}(::Type{T}) = Vector{T}
vectortype{T}(::Type{Nullable{T}}) = NullableVector{T}

coltype(sch::Data.Schema, i::Integer) = vectortype(Data.types(sch)[i])
function coltypes(sch::Data.Schema, cols::AbstractVector{<:Integer})
    DataType[coltype(sch, c) for c ∈ cols]
end
coltypes(sch::Data.Schema) = coltypes(sch, 1:length(Data.header(sch)))

# this will hopefully get implemented in DataStreams some day
function streamfrom{T}(src, ::Type{Data.Column}, ::Type{Vector{T}},
                       rows::AbstractVector{<:Integer}, col::Integer)
    [Data.streamfrom(src, Data.Field, T, i, col) for i ∈ rows]::Vector{T}
end
function streamfrom{T}(src, ::Type{Data.Column}, ::Type{NullableVector{T}},
                       rows::AbstractVector{<:Integer}, col::Integer)
    [Data.streamfrom(src, Data.Field, Nullable{T}, i, col) for i ∈ rows]::NullableVector{T}
end

function sift{T}(src, f::Function, ::Type{T}, i::Integer, a::Integer, b::Integer)
    f.(streamfrom(src, Data.Column, T, a:b, i))::AbstractVector{Bool}
end


