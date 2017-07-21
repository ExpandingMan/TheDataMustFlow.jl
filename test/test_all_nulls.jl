using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils
using BenchmarkTools
using Estuaries

const filename = "sample_nulls.feather"
const batch_size = 1024

src = Feather.Source(filename)
src_sch = Data.schema(src)
nrows = size(src, 1)
est = Estuaries.Source(src)
# data = featherRead(filename)
# nrows = size(data,1)
# data[:γ] = Nullable{Float32}()
# data[:δ] = Nullable{Float32}()
# src = data
# sink = data

# create Surveyor
sv = @btime surveyall(src, 1:nrows, Header1=(i -> i % 2 == 0), Header2=(i -> i % 3 == 0),
                      pool_cols=[:Header2])
idx = sv[]


# construct Harvester
h = Harvester(src, Float32, [:Header2, :B], null_replacement=(() -> -rand()),
              categories=Dict(:Header2=>getpool(sv, :Header2)))
harvest = harvester(h)

# create a sink to put data back into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
header = [Symbol.(Data.header(src_sch)); :γ; :δ]
sink = DataTable()
for (col,dtype) ∈ zip(header,dtypes)
    sink[col] = NullableVector{dtype}(nrows)
end


# migrate everything
@btime migrate!(1:nrows, src=>sink)

# TODO simplify sower interface
# create Sower
sow! = sower(sink, [:γ, :δ])
# main loop
@btime for sidx ∈ batchiter(idx, batch_size)
    X, = harvest(sidx)
    y = -X
    sow!(sidx, y)
end





