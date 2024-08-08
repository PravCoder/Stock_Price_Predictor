/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Rewriters                                                                  *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: decompose_patterns.td                                                *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/


static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns0(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!((attr.cast<StringAttr>().getValue() == "NONE"))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": ";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns1(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!((attr.cast<StringAttr>().getValue() == "RELU6"))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": ";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns2(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!(((::llvm::isa<::mlir::FloatAttr>(attr))) && ((::llvm::cast<::mlir::FloatAttr>(attr).getType().isF32())))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": 32-bit float attribute";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns3(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!(((::llvm::isa<::mlir::IntegerAttr>(attr))) && ((::llvm::cast<::mlir::IntegerAttr>(attr).getType().isSignlessInteger(64))))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": 64-bit signless integer attribute";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns4(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!((attr.cast<StringAttr>().getValue() == "RELU_N1_TO_1"))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": ";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_attr_constraint_decompose_patterns5(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Attribute attr,
    ::llvm::StringRef failureStr) {
  if (!((attr.cast<StringAttr>().getValue() == "RELU"))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": ";
    });
  }
  return ::mlir::success();
}
static ::mlir::LogicalResult static_dag_matcher_0(::mlir::PatternRewriter &rewriter, ::mlir::Operation *op0, ::llvm::SmallVector<::mlir::Operation *, 4> &tblgen_ops, ::mlir::Attribute &scale) {
  (void)tblgen_ops;
    ::mlir::Attribute arg1_0;
    if (!(!::mlir::failed(::mlir::success(::mlir::matchPattern(op0->getResult(0), ::mlir::m_Constant(&arg1_0)))))){
      return rewriter.notifyMatchFailure(op0, [&](::mlir::Diagnostic &diag) {
        diag << "::mlir::success(::mlir::matchPattern(op0->getResult(0), ::mlir::m_Constant(&arg1_0))) return ::mlir::failure";
      });
    }
    scale = arg1_0;
    if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns2(rewriter, op0, arg1_0, "operand 0 of native code call '::mlir::success(::mlir::matchPattern($_self->getResult(0), ::mlir::m_Constant(&$0)))' failed to satisfy constraint: '32-bit float attribute'"))) {
      return ::mlir::failure();
    }
  return ::mlir::success();
}

static ::mlir::LogicalResult static_dag_matcher_1(::mlir::PatternRewriter &rewriter, ::mlir::Operation *op0, ::llvm::SmallVector<::mlir::Operation *, 4> &tblgen_ops, ::mlir::Attribute &zp) {
  (void)tblgen_ops;
    ::mlir::Attribute arg1_0;
    if (!(!::mlir::failed(::mlir::success(::mlir::matchPattern(op0->getResult(0), ::mlir::m_Constant(&arg1_0)))))){
      return rewriter.notifyMatchFailure(op0, [&](::mlir::Diagnostic &diag) {
        diag << "::mlir::success(::mlir::matchPattern(op0->getResult(0), ::mlir::m_Constant(&arg1_0))) return ::mlir::failure";
      });
    }
    zp = arg1_0;
    if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns3(rewriter, op0, arg1_0, "operand 0 of native code call '::mlir::success(::mlir::matchPattern($_self->getResult(0), ::mlir::m_Constant(&$0)))' failed to satisfy constraint: '64-bit signless integer attribute'"))) {
      return ::mlir::failure();
    }
  return ::mlir::success();
}

/* Generated from:
    tensorflow/compiler/mlir/tfr/passes/decompose_patterns.td:26
*/
struct QuantActRangeNonePattern : public ::mlir::RewritePattern {
  QuantActRangeNonePattern(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("tfr.quant_act_range", 2, context, {"arith.constant", "tfr.constant_tensor"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range zp(op0->getOperands());
    ::mlir::Operation::operand_range scale(op0->getOperands());
    Attribute act;
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::TFR::TFRQuantActRangeOp>(op0); (void)castedOp0;
    {
      auto *op1 = (*castedOp0.getODSOperands(0).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 0 of castedOp0";
        });
      }
      auto castedOp1 = ::llvm::dyn_cast<::mlir::TFR::ConstOp>(op1); (void)castedOp1;
      if (!(castedOp1)){
        return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
          diag << "castedOp1 is not ::mlir::TFR::ConstOp type";
        });
      }
      {
        auto tblgen_attr = op1->getAttrOfType<Attribute>("value");(void)tblgen_attr;
        if (!(tblgen_attr)){
          return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
            diag << "expected op 'tfr.constant' to have attribute 'value' of type 'Attribute'";
          });
        }
        if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns0(rewriter, op1, tblgen_attr, "op 'tfr.constant' attribute 'value' failed to satisfy constraint: ''"))) {
          return ::mlir::failure();
        }
        act = tblgen_attr;
      }
      tblgen_ops.push_back(op1);
    }
    scale = castedOp0.getODSOperands(1);
    zp = castedOp0.getODSOperands(2);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc(), tblgen_ops[1]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_0;
    {
      tblgen_ConstantOp_0 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        rewriter.getIntegerAttr(rewriter.getIntegerType(32), -128)
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_1;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_0.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_1 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_1.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }
    ::mlir::arith::ConstantOp tblgen_ConstantOp_2;
    {
      tblgen_ConstantOp_2 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        rewriter.getIntegerAttr(rewriter.getIntegerType(32), 127)
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_3;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_2.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(1)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_3 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_3.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    tensorflow/compiler/mlir/tfr/passes/decompose_patterns.td:42
*/
struct QuantActRangeRelu6Pattern : public ::mlir::RewritePattern {
  QuantActRangeRelu6Pattern(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("tfr.quant_act_range", 2, context, {"arith.constant", "tfr.constant_tensor"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Attribute zp;
    ::mlir::Attribute scale;
    Attribute act;
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::TFR::TFRQuantActRangeOp>(op0); (void)castedOp0;
    {
      auto *op1 = (*castedOp0.getODSOperands(0).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 0 of castedOp0";
        });
      }
      auto castedOp1 = ::llvm::dyn_cast<::mlir::TFR::ConstOp>(op1); (void)castedOp1;
      if (!(castedOp1)){
        return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
          diag << "castedOp1 is not ::mlir::TFR::ConstOp type";
        });
      }
      {
        auto tblgen_attr = op1->getAttrOfType<Attribute>("value");(void)tblgen_attr;
        if (!(tblgen_attr)){
          return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
            diag << "expected op 'tfr.constant' to have attribute 'value' of type 'Attribute'";
          });
        }
        if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns1(rewriter, op1, tblgen_attr, "op 'tfr.constant' attribute 'value' failed to satisfy constraint: ''"))) {
          return ::mlir::failure();
        }
        act = tblgen_attr;
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(1).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 1 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_0(rewriter, op1, tblgen_ops, scale))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(2).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 2 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_1(rewriter, op1, tblgen_ops, zp))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc(), tblgen_ops[1]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = TFR::Quantize(0.0f, scale, zp, rewriter); (void)nativeVar_0;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_1;
    {
      tblgen_ConstantOp_1 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        /*value=*/nativeVar_0
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_1.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_2 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }
    auto nativeVar_3 = TFR::Quantize(6.0f, scale, zp, rewriter); (void)nativeVar_3;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_4;
    {
      tblgen_ConstantOp_4 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        /*value=*/nativeVar_3
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_5;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_4.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(1)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_5 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_5.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    tensorflow/compiler/mlir/tfr/passes/decompose_patterns.td:52
*/
struct QuantActRangeReluN1To1Pattern : public ::mlir::RewritePattern {
  QuantActRangeReluN1To1Pattern(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("tfr.quant_act_range", 2, context, {"arith.constant", "tfr.constant_tensor"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Attribute zp;
    ::mlir::Attribute scale;
    Attribute act;
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::TFR::TFRQuantActRangeOp>(op0); (void)castedOp0;
    {
      auto *op1 = (*castedOp0.getODSOperands(0).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 0 of castedOp0";
        });
      }
      auto castedOp1 = ::llvm::dyn_cast<::mlir::TFR::ConstOp>(op1); (void)castedOp1;
      if (!(castedOp1)){
        return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
          diag << "castedOp1 is not ::mlir::TFR::ConstOp type";
        });
      }
      {
        auto tblgen_attr = op1->getAttrOfType<Attribute>("value");(void)tblgen_attr;
        if (!(tblgen_attr)){
          return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
            diag << "expected op 'tfr.constant' to have attribute 'value' of type 'Attribute'";
          });
        }
        if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns4(rewriter, op1, tblgen_attr, "op 'tfr.constant' attribute 'value' failed to satisfy constraint: ''"))) {
          return ::mlir::failure();
        }
        act = tblgen_attr;
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(1).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 1 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_0(rewriter, op1, tblgen_ops, scale))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(2).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 2 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_1(rewriter, op1, tblgen_ops, zp))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc(), tblgen_ops[1]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = TFR::Quantize(-1.0f, scale, zp, rewriter); (void)nativeVar_0;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_1;
    {
      tblgen_ConstantOp_1 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        /*value=*/nativeVar_0
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_1.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_2 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }
    auto nativeVar_3 = TFR::Quantize(1.0f, scale, zp, rewriter); (void)nativeVar_3;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_4;
    {
      tblgen_ConstantOp_4 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        /*value=*/nativeVar_3
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_5;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_4.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(1)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_5 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_5.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    tensorflow/compiler/mlir/tfr/passes/decompose_patterns.td:33
*/
struct QuantActRangeReluPattern : public ::mlir::RewritePattern {
  QuantActRangeReluPattern(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("tfr.quant_act_range", 2, context, {"arith.constant", "tfr.constant_tensor"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Attribute zp;
    ::mlir::Attribute scale;
    Attribute act;
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::TFR::TFRQuantActRangeOp>(op0); (void)castedOp0;
    {
      auto *op1 = (*castedOp0.getODSOperands(0).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 0 of castedOp0";
        });
      }
      auto castedOp1 = ::llvm::dyn_cast<::mlir::TFR::ConstOp>(op1); (void)castedOp1;
      if (!(castedOp1)){
        return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
          diag << "castedOp1 is not ::mlir::TFR::ConstOp type";
        });
      }
      {
        auto tblgen_attr = op1->getAttrOfType<Attribute>("value");(void)tblgen_attr;
        if (!(tblgen_attr)){
          return rewriter.notifyMatchFailure(op1, [&](::mlir::Diagnostic &diag) {
            diag << "expected op 'tfr.constant' to have attribute 'value' of type 'Attribute'";
          });
        }
        if(::mlir::failed(__mlir_ods_local_attr_constraint_decompose_patterns5(rewriter, op1, tblgen_attr, "op 'tfr.constant' attribute 'value' failed to satisfy constraint: ''"))) {
          return ::mlir::failure();
        }
        act = tblgen_attr;
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(1).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 1 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_0(rewriter, op1, tblgen_ops, scale))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }
    {
      auto *op1 = (*castedOp0.getODSOperands(2).begin()).getDefiningOp();
      if (!(op1)){
        return rewriter.notifyMatchFailure(castedOp0, [&](::mlir::Diagnostic &diag) {
          diag << "There's no operation that defines operand 2 of castedOp0";
        });
      }
      if(::mlir::failed(static_dag_matcher_1(rewriter, op1, tblgen_ops, zp))) {
        return ::mlir::failure();
      }
      tblgen_ops.push_back(op1);
    }

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc(), tblgen_ops[1]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = TFR::Quantize(0.0f, scale, zp, rewriter); (void)nativeVar_0;
    ::mlir::arith::ConstantOp tblgen_ConstantOp_1;
    {
      tblgen_ConstantOp_1 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        /*value=*/nativeVar_0
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_1.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_2 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }
    ::mlir::arith::ConstantOp tblgen_ConstantOp_3;
    {
      tblgen_ConstantOp_3 = rewriter.create<::mlir::arith::ConstantOp>(odsLoc,
        rewriter.getIntegerAttr(rewriter.getIntegerType(32), 127)
      );
    }
    ::mlir::TFR::ConstantTensorOp tblgen_ConstantTensorOp_4;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ConstantOp_3.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(1)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantTensorOp_4 = rewriter.create<::mlir::TFR::ConstantTensorOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantTensorOp_4.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

void LLVM_ATTRIBUTE_UNUSED populateWithGenerated(::mlir::RewritePatternSet &patterns) {
  patterns.add<QuantActRangeNonePattern>(patterns.getContext());
  patterns.add<QuantActRangeRelu6Pattern>(patterns.getContext());
  patterns.add<QuantActRangeReluN1To1Pattern>(patterns.getContext());
  patterns.add<QuantActRangeReluPattern>(patterns.getContext());
}
