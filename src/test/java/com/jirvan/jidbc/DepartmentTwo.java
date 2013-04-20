/*

Copyright (c) 2013, Jirvan Pty Ltd
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of Jirvan Pty Ltd nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.jirvan.jidbc;

import java.math.*;
import java.util.*;

public class DepartmentTwo {

    private Long departmentId;

    private String departmentAbbr;

    private String departmentName;

    private String thingyType;

    private Integer thingyNumber;

    private BigDecimal anotherThingy;

    private Date inactivatedDatetime;

    @Id(generatorSequence = "common_id_sequence")
    public Long getDepartmentId() {
        return departmentId;
    }

    public void setDepartmentId(Long departmentId) {
        this.departmentId = departmentId;
    }

    public String getDepartmentAbbr() {
        return departmentAbbr;
    }

    public void setDepartmentAbbr(String departmentAbbr) {
        this.departmentAbbr = departmentAbbr;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public String getThingyType() {
        return thingyType;
    }

    public void setThingyType(String thingyType) {
        this.thingyType = thingyType;
    }

    public Integer getThingyNumber() {
        return thingyNumber;
    }

    public void setThingyNumber(Integer thingyNumber) {
        this.thingyNumber = thingyNumber;
    }

    public BigDecimal getAnotherThingy() {
        return anotherThingy;
    }

    public void setAnotherThingy(BigDecimal anotherThingy) {
        this.anotherThingy = anotherThingy;
    }

    public Date getInactivatedDatetime() {
        return inactivatedDatetime;
    }

    public void setInactivatedDatetime(Date inactivatedDatetime) {
        this.inactivatedDatetime = inactivatedDatetime;
    }
}
