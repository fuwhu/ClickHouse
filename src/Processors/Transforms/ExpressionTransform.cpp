#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & expression, bool is_skip_indices_expression, StorageMetadataPtr metadata_snapshot)
{
    if (is_skip_indices_expression && metadata_snapshot && metadata_snapshot->hasImplicitColumn())
        MergeTreeDataWriter::fillMissingImplicitColumnsForSkipIndices(header, metadata_snapshot, metadata_snapshot->secondary_indices);
    return expression.updateHeader(std::move(header));
}


ExpressionTransform::ExpressionTransform(
    const Block & header_, ExpressionActionsPtr expression_, bool is_skip_indices_expression_, StorageMetadataPtr metadata_snapshot_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG(), is_skip_indices_expression_, metadata_snapshot_), false)
    , expression(std::move(expression_))
    , is_skip_indices_expression(is_skip_indices_expression_)
    , metadata_snapshot(metadata_snapshot_)
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (is_skip_indices_expression && metadata_snapshot && metadata_snapshot->hasImplicitColumn())
        MergeTreeDataWriter::fillMissingImplicitColumnsForSkipIndices(block, metadata_snapshot, metadata_snapshot->secondary_indices);
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
