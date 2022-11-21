#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & expression)
{
    return expression.updateHeader(std::move(header));
}


ExpressionTransform::ExpressionTransform(
    const Block & header_, ExpressionActionsPtr expression_, bool is_skip_indices_expression_, StorageMetadataPtr metadata_snapshot_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG()), false)
    , expression(std::move(expression_))
    , is_skip_indices_expression(is_skip_indices_expression_)
    , metadata_snapshot(metadata_snapshot_)
{
    auto & mutable_header = const_cast<Block &>(header_);
    if (is_skip_indices_expression && metadata_snapshot->hasImplicitColumn())
    {
        MergeTreeDataWriter::fillMissingImplicitColumnsForSkipIndices(mutable_header, metadata_snapshot);
    }
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
}

ConvertingTransform::ConvertingTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, ExpressionTransform::transformHeader(header_, expression_->getActionsDAG()))
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
