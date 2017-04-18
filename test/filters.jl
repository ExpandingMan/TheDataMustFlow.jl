using DataTables
using DataStreams
using TheDataMustFlow

const nrows = 10000

src = DataTable(A=1:nrows, B=1:nrows, C=rand(nrows))
src[12, :A] = Nullable()

filter = StreamFilter(src, [:A, :B], Function[i -> (i % 3 == 0), i -> (i % 2) == 0])

@time idx = index(filter, 1:nrows)
@time idx = index(filter, 1:nrows)

