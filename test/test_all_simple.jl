using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils
using BenchmarkTools
using Estuaries

const filename = "sample.feather"
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


# create StreamFilter
# sfilter = streamfilter(src, Header1=(i -> i % 2 == 0),
#                        Header2=(i -> i % 3 == 0))
# collect all valid indices
# idx = @btime filterall(src, 1:nrows, Header1=(i -> i % 2 == 0),
#                        Header2=(i -> i % 3 == 0))

# create Surveyor
# sv = surveyor(src, Header1=(i -> i % 2 == 0),
#               Header2=(i -> i % 3 == 0))
# collect all avlid indices
sv = @btime surveyall(src, 1:nrows, Header1=(i -> i % 2 == 0),
                      Header2=(i -> i % 3 == 0))

# # construct Harvester
# harvest = harvester(src, Float64, [:A, :C], [:B, :D])
#
#
# # create a sink to put data back into
# dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
# header = [Symbol.(Data.header(src_sch)); :γ; :δ]
# sink = DataTable(dtypes, header, nrows)
#
# # migrate everything
# @btime migrate!(1:nrows, src=>sink)
#
# # create Sower
# sow! = sower(sink, [:γ, :δ])
#
# @btime for sidx ∈ batchiter(idx, batch_size)
#     X, y = harvest(sidx)
#     y = -X
#     sow!(sidx, y)
# end

