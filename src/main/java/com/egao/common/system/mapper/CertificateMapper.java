package com.egao.common.system.mapper;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 Mapper 接口
 * </p>
 *
 * @author hs
 * @since 2020-11-11
 */
@Component
public interface CertificateMapper{

    List<Map<String, Object>> selectCertificate(Map map);

    List<Map<String, Object>> selectAllCertificate(Map map);

    Map<String, Object> selectCertificateById(@Param("id") Long id);

    void insertCertificate(Map map);

    void updateCertificate(Map map);

    void deleteCertificate(@Param("id") Long id);

}
