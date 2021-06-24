package com.egao.common.system.service.impl;

import com.egao.common.system.mapper.CertificateMapper;
import com.egao.common.system.service.CertificateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 服务实现类
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class CertificateServiceImpl implements CertificateService {

    @Autowired
    public CertificateMapper certificateMapper;

    @Override
    public List<Map<String, Object>> selectCertificate(Map map){
        List<Map<String, Object>> certificateList = certificateMapper.selectCertificate(map);
        return certificateList;
    }

    @Override
    public List<Map<String, Object>> selectAllCertificate(Map map){
        List<Map<String, Object>> certificateList = certificateMapper.selectAllCertificate(map);
        return certificateList;
    }


    @Override
    public Map<String, Object> selectCertificateById(Long id){
        Map<String, Object> certificateMap = certificateMapper.selectCertificateById(id);
        return certificateMap;
    }

    @Override
    public void insertCertificate(Map map){
        certificateMapper.insertCertificate(map);

    }

    @Override
    public void updateCertificate(Map map){
        certificateMapper.updateCertificate(map);
    }

    @Override
    public void deleteCertificate(Long id){
        certificateMapper.deleteCertificate(id);
    }


}
