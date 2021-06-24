package com.egao.common.system.service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 服务类
 * </p>
 *
 * @author hs
 * @since 2019-10-10
 */
public interface CertificateService{

    List<Map<String, Object>> selectCertificate(Map map);

    List<Map<String, Object>> selectAllCertificate(Map map);

    Map<String, Object> selectCertificateById(Long id);

    void insertCertificate(Map map);

    void updateCertificate(Map map);

    void deleteCertificate(Long id);





}
