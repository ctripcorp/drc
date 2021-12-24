package com.ctrip.framework.drc.core.server.config.cm.dto;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class SchemasHistoryDto {
    public List<SchemasHistoryDeltaDto> getSchemasHistoryDeltas() {
        return schemasHistoryDeltas;
    }

    public void setSchemasHistoryDeltas(List<SchemasHistoryDeltaDto> schemasHistoryDeltas) {
        this.schemasHistoryDeltas = schemasHistoryDeltas;
    }

    List<SchemasHistoryDeltaDto> schemasHistoryDeltas;

    public static SchemasHistoryDto create() {
        SchemasHistoryDto newObject = new SchemasHistoryDto();
        newObject.setSchemasHistoryDeltas(Lists.newArrayList());
        return newObject;
    }

    public boolean add(SchemasHistoryDeltaDto delta) {
        return schemasHistoryDeltas.add(delta);
    }

    public boolean merge(SchemasHistoryDto history) {
        return schemasHistoryDeltas.addAll(history.getSchemasHistoryDeltas());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchemasHistoryDto)) return false;
        SchemasHistoryDto that = (SchemasHistoryDto) o;
        return Objects.equals(schemasHistoryDeltas, that.schemasHistoryDeltas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemasHistoryDeltas);
    }
}
